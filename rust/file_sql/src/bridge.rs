// Copyright (c) 2026 OceanBase.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! C++ bridge. Every call returns a JSON envelope:
//! either the requested payload or {"error": "..."} for a diagnostic that the
//! SQL layer turns into a statement error.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Value};

use crate::reader::{parse_options, CsvWriter, Reader};

#[cxx::bridge(namespace = "oceanbase::file_sql")]
mod ffi {
    extern "Rust" {
        /// Opens a file table. Returns {"handle":N,"columns":[{"name":..,"type":..}]}.
        fn open_reader(path: &str, format: u32, options_json: &str, expected_version: &str, projection_json: &str) -> String;
        /// Streams one row batch. Returns {"rows":[[..]],"eof":bool}.
        fn reader_next(handle: u64, max_rows: u32) -> String;
        fn close_reader(handle: u64);
        /// Expands a glob pattern. Returns {"files":["/abs/path", ...]}.
        fn glob_files(pattern: &str) -> String;
        /// COPY export helpers.
        fn open_csv_writer(path: &str, options_json: &str) -> String;
        fn csv_writer_push(handle: u64, header_json: &str, rows_json: &str) -> String;
        fn finish_csv_writer(handle: u64) -> String;
        fn close_csv_writer(handle: u64);
    }
}

enum Entry {
    Reader(Reader),
    Writer(CsvWriter),
}

struct Registry {
    entries: HashMap<u64, Entry>,
    next_handle: u64,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        Mutex::new(Registry {
            entries: HashMap::new(),
            next_handle: 1,
        })
    })
}

fn lock() -> std::sync::MutexGuard<'static, Registry> {
    registry().lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn error(message: impl Into<String>) -> String {
    json!({ "error": message.into() }).to_string()
}

fn open_reader(path: &str, format: u32, options_json: &str, expected_version: &str, projection_json: &str) -> String {
    let options = match parse_options(options_json) {
        Ok(options) => options,
        Err(message) => return error(message),
    };
    let projection: Option<Vec<usize>> = match serde_json::from_str(projection_json) {
        Ok(value) => value, Err(e) => return error(format!("invalid file projection: {e}")),
    };
    match Reader::open(path, format, &options, expected_version, projection.as_deref()) {
        Err(message) => error(message),
        Ok((reader, columns)) => {
            let version = reader.version().to_string();
            let handle = {
                let mut guard = lock();
                let handle = guard.next_handle;
                guard.next_handle += 1;
                guard.entries.insert(handle, Entry::Reader(reader));
                handle
            };
            let columns: Vec<Value> = columns
                .iter()
                .map(|column| json!({ "name": column.name, "type": column.ty.code(), "nullable": column.nullable, "scale": column.scale }))
                .collect();
            json!({ "handle": handle, "columns": columns, "version": version }).to_string()
        }
    }
}

fn reader_next(handle: u64, max_rows: u32) -> String {
    let mut guard = lock();
    let entry = match guard.entries.get_mut(&handle) {
        Some(Entry::Reader(reader)) => reader,
        Some(_) => return error("file reader handle is not readable"),
        None => return error("file reader handle is closed"),
    };
    match entry.next_batch(max_rows as usize) {
        Err(message) => error(message),
        Ok((rows, eof)) => {
            let rows: Vec<Value> = rows
                .into_iter()
                .map(|row| Value::Array(row.into_iter().map(Value::from).collect()))
                .collect();
            json!({ "rows": rows, "eof": eof }).to_string()
        }
    }
}

fn close_reader(handle: u64) {
    lock().entries.remove(&handle);
}

fn glob_files(pattern: &str) -> String {
    let paths = match glob::glob(pattern) {
        Ok(paths) => paths,
        Err(message) => return error(format!("invalid glob pattern '{pattern}': {message}")),
    };
    let mut files: Vec<String> = Vec::new();
    for path in paths {
        match path {
            Ok(path) if path.is_file() => files.push(path.to_string_lossy().to_string()),
            Ok(_) => {},
            Err(message) => return error(format!("cannot resolve glob entry: {message}")),
        }
    }
    files.sort();
    json!({ "files": files }).to_string()
}

fn open_csv_writer(path: &str, options_json: &str) -> String {
    let options = match parse_options(options_json) {
        Ok(options) => options,
        Err(message) => return error(message),
    };
    match CsvWriter::open(path, &options) {
        Err(message) => error(message),
        Ok(writer) => {
            let handle = {
                let mut guard = lock();
                let handle = guard.next_handle;
                guard.next_handle += 1;
                guard.entries.insert(handle, Entry::Writer(writer));
                handle
            };
            json!({ "handle": handle }).to_string()
        }
    }
}

fn parse_header(text: &str) -> Result<Option<Vec<String>>, String> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text).map_err(|e| format!("invalid header: {e}"))?;
    let items = value
        .as_array()
        .ok_or_else(|| "header must be a json array".to_string())?;
    let mut header = Vec::with_capacity(items.len());
    for item in items {
        header.push(item.as_str().unwrap_or_default().to_string());
    }
    Ok(Some(header))
}

fn parse_rows(text: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_str(text).map_err(|e| format!("invalid rows: {e}"))?;
    let items = value
        .as_array()
        .ok_or_else(|| "rows must be a json array".to_string())?;
    let mut rows = Vec::with_capacity(items.len());
    for item in items {
        let cells = item
            .as_array()
            .ok_or_else(|| "each row must be a json array".to_string())?;
        let mut row = Vec::with_capacity(cells.len());
        for cell in cells {
            if cell.is_null() {
                row.push(None);
            } else if let Some(text) = cell.as_str() {
                row.push(Some(text.to_string()));
            } else {
                row.push(Some(cell.to_string()));
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

fn csv_writer_push(handle: u64, header_json: &str, rows_json: &str) -> String {
    let header = match parse_header(header_json) {
        Ok(header) => header,
        Err(message) => return error(message),
    };
    let rows = match parse_rows(rows_json) {
        Ok(rows) => rows,
        Err(message) => return error(message),
    };
    let mut guard = lock();
    let writer = match guard.entries.get_mut(&handle) {
        Some(Entry::Writer(writer)) => writer,
        Some(_) => return error("file writer handle is not writable"),
        None => return error("file writer handle is closed"),
    };
    match writer.push(&rows, header.as_deref()) {
        Err(message) => error(message),
        Ok(()) => json!({ "ok": true }).to_string(),
    }
}

fn close_csv_writer(handle: u64) {
    lock().entries.remove(&handle);
}

fn finish_csv_writer(handle: u64) -> String {
    let mut guard = lock();
    match guard.entries.remove(&handle) {
        Some(Entry::Writer(mut writer)) => match writer.finish() {
            Err(message) => error(message),
            Ok(()) => json!({ "ok": true }).to_string(),
        },
        Some(other) => {
            guard.entries.insert(handle, other);
            error("file writer handle is not writable")
        }
        None => error("file writer handle is closed"),
    }
}
