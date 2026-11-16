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

//! Streaming readers for CSV, JSONL and Parquet files.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

use parquet::basic::{ConvertedType, Repetition, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use parquet::record::reader::RowIter;

use crate::infer::{classify, widen, ColType};

pub const FORMAT_CSV: u32 = 0;
pub const FORMAT_JSON: u32 = 1;
pub const FORMAT_PARQUET: u32 = 2;

const MAX_ROW_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_COLUMNS: usize = 4096;

fn file_version(metadata: &std::fs::Metadata) -> String {
    #[cfg(unix)] {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}:{}:{}:{}:{}:{}", metadata.dev(), metadata.ino(), metadata.len(),
                metadata.mtime(), metadata.mtime_nsec(), metadata.ctime(), metadata.ctime_nsec())
    }
    #[cfg(not(unix))] {
        format!("{}:{:?}:{:?}", metadata.len(), metadata.modified(), metadata.created())
    }
}

struct FileState {
    file: File,
    path: PathBuf,
    version: String,
}

impl FileState {
    fn check(&self) -> Result<(), String> {
        let opened = self.file.metadata().map_err(|e| format!("cannot inspect '{}': {e}", self.path.display()))?;
        let current = std::fs::metadata(&self.path).map_err(|e| format!("file changed '{}': {e}", self.path.display()))?;
        if file_version(&opened) != self.version || file_version(&current) != self.version {
            return Err(format!("file changed during query: '{}'", self.path.display()));
        }
        Ok(())
    }
}

// Bound a CSV record without replacing the csv crate's parser.
struct RecordReader {
    file: File,
    remaining: usize,
}
impl Read for RecordReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "CSV record exceeds 16 MiB"));
        }
        let size = buf.len().min(self.remaining);
        let read = self.file.read(&mut buf[..size])?;
        self.remaining -= read;
        Ok(read)
    }
}

fn read_csv_record(reader: &mut csv::Reader<RecordReader>, record: &mut csv::StringRecord) -> Result<bool, String> {
    reader.get_mut().remaining = MAX_ROW_BYTES + 8192;
    let found = reader.read_record(record).map_err(|e| format!("invalid CSV record: {e}"))?;
    if record.as_slice().len() > MAX_ROW_BYTES || record.len() > MAX_COLUMNS {
        return Err(format!("CSV record exceeds limits at {:?}", record.position()));
    }
    Ok(found)
}

fn read_json_line(reader: &mut BufReader<File>, line_no: &mut usize) -> Result<Option<String>, String> {
    let mut line = String::new();
    let size = (&mut *reader).take((MAX_ROW_BYTES + 1) as u64).read_line(&mut line)
        .map_err(|e| format!("cannot read JSON line {}: {e}", *line_no + 1))?;
    if size == 0 { return Ok(None); }
    *line_no += 1;
    if size > MAX_ROW_BYTES { return Err(format!("JSON line {} exceeds 16 MiB", *line_no)); }
    Ok(Some(line))
}

fn row_bytes(row: &[Option<String>]) -> Result<usize, String> {
    let bytes: usize = row.iter().filter_map(Option::as_ref).map(String::len).sum::<usize>()
        + row.len() * std::mem::size_of::<Option<String>>();
    if bytes > MAX_ROW_BYTES { return Err("file row exceeds 16 MiB".into()); }
    Ok(bytes)
}

fn datetime_scale(text: &str) -> u8 {
    text.split_once('.').map_or(0, |(_, fraction)| fraction.len().min(6) as u8)
}


#[derive(Clone, Debug)]
pub struct Options {
    pub header: bool,
    pub delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub nullstr: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            header: true,
            delimiter: b',',
            quote: b'"',
            escape: b'"',
            nullstr: String::new(),
        }
    }
}

fn single_byte(value: &serde_json::Value, name: &str) -> Result<u8, String> {
    let text = value
        .as_str()
        .ok_or_else(|| format!("option '{name}' must be a one character string"))?;
    let bytes = text.as_bytes();
    if bytes.len() != 1 {
        return Err(format!("option '{name}' must be a one character string"));
    }
    Ok(bytes[0])
}

pub fn parse_options(text: &str) -> Result<Options, String> {
    let mut options = Options::default();
    if text.trim().is_empty() {
        return Ok(options);
    }
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("invalid reader options: {e}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "reader options must be a json object".to_string())?;
    for (key, item) in object {
        match key.as_str() {
            "header" => {
                options.header = item
                    .as_bool()
                    .ok_or_else(|| "option 'header' must be a boolean".to_string())?
            }
            "delimiter" => options.delimiter = single_byte(item, "delimiter")?,
            "quote" => options.quote = single_byte(item, "quote")?,
            "escape" => options.escape = single_byte(item, "escape")?,
            "nullstr" => {
                options.nullstr = item
                    .as_str()
                    .ok_or_else(|| "option 'nullstr' must be a string".to_string())?
                    .to_string()
            }
            "sample_size" => {
                let size = item
                    .as_i64()
                    .ok_or_else(|| "option 'sample_size' must be an integer".to_string())?;
                if size != -1 {
                    return Err(
                        "option 'sample_size' only supports -1 for full schema inference"
                            .to_string(),
                    );
                }
            }
            other => return Err(format!("unknown reader option '{other}'")),
        }
    }
    Ok(options)
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: ColType,
    pub nullable: bool,
    pub scale: u8,
}

fn unique_name(seen: &mut HashSet<String>, base: &str, index: usize) -> String {
    let candidate = if base.is_empty() {
        format!("column{index}")
    } else {
        base.to_string()
    };
    if seen.insert(candidate.clone()) {
        return candidate;
    }
    let mut ordinal = 2;
    loop {
        let next = format!("{candidate}__{ordinal}");
        if seen.insert(next.clone()) {
            return next;
        }
        ordinal += 1;
    }
}

fn csv_builder(options: &Options) -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    builder
        .has_headers(options.header)
        .delimiter(options.delimiter)
        .quote(options.quote)
        .double_quote(options.escape == options.quote)
        .flexible(false);
    if options.escape != 0 && options.escape != options.quote {
        builder.escape(Some(options.escape));
    }
    builder
}

fn csv_columns(mut file: File, path: &str, options: &Options) -> Result<(File, Vec<Column>), String> {
    file.rewind().map_err(|e| format!("cannot rewind '{path}': {e}"))?;
    let mut reader = csv_builder(options).from_reader(RecordReader { file, remaining: MAX_ROW_BYTES + 8192 });
    let header = reader.headers().map_err(|e| format!("invalid CSV header in '{path}': {e}"))?.clone();
    if header.len() > MAX_COLUMNS || header.as_slice().len() > MAX_ROW_BYTES {
        return Err(format!("CSV header exceeds limits in '{path}'"));
    }
    let mut seen = HashSet::new();
    let mut columns: Vec<Column> = header.iter().enumerate().map(|(i, name)| Column {
        name: unique_name(&mut seen, if options.header { name } else { "" }, i),
        ty: ColType::Null, nullable: false, scale: 0,
    }).collect();
    let mut record = csv::StringRecord::new();
    while read_csv_record(&mut reader, &mut record)? {
        for (column, text) in columns.iter_mut().zip(record.iter()) {
            let ty = classify(text, &options.nullstr);
            column.nullable |= ty == ColType::Null;
            column.ty = widen(column.ty, ty);
            if ty == ColType::Datetime { column.scale = column.scale.max(datetime_scale(text)); }
        }
    }
    let mut file = reader.into_inner().file;
    file.rewind().map_err(|e| format!("cannot rewind '{path}': {e}"))?;
    Ok((file, columns))
}

pub struct CsvReader {
    reader: csv::Reader<RecordReader>,
    nullstr: String,
    finished: bool,
}

impl CsvReader {
    fn open(file: File, path: &str, options: &Options) -> Result<(CsvReader, Vec<Column>), String> {
        let (file, columns) = csv_columns(file, path, options)?;
        let reader = csv_builder(options).from_reader(RecordReader { file, remaining: MAX_ROW_BYTES + 8192 });
        Ok((CsvReader { reader, nullstr: options.nullstr.clone(), finished: false }, columns))
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<(Vec<Vec<Option<String>>>, bool), String> {
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut record = csv::StringRecord::new();
        while !self.finished && rows.len() < max_rows && bytes < MAX_BATCH_BYTES - MAX_ROW_BYTES {
            if !read_csv_record(&mut self.reader, &mut record)? { self.finished = true; break; }
            let row: Vec<Option<String>> = record.iter().map(|text| {
                if text.is_empty() || (!self.nullstr.is_empty() && text == self.nullstr) { None }
                else { Some(text.to_string()) }
            }).collect();
            bytes += row_bytes(&row)?;
            rows.push(row);
        }
        Ok((rows, self.finished))
    }
}

pub struct JsonReader {
    reader: BufReader<File>,
    keys: Vec<String>,
    line_no: usize,
    finished: bool,
}

fn json_value_type(value: &serde_json::Value, path: &str) -> Result<ColType, String> {
    match value {
        serde_json::Value::Null => Ok(ColType::Null),
        serde_json::Value::Bool(_) => Ok(ColType::Bool),
        serde_json::Value::Number(number) => {
            if number.as_i64().is_some() {
                Ok(ColType::Int)
            } else if number.as_u64().is_some() {
                Ok(ColType::Varchar)
            } else {
                Ok(ColType::Double)
            }
        }
        serde_json::Value::String(text) => Ok(match classify(text, "") {
            ColType::Date => ColType::Date, ColType::Datetime => ColType::Datetime, _ => ColType::Varchar,
        }),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => Err(format!(
            "nested json values are not supported (field '{path}')"
        )),
    }
}

fn json_columns(file: File, path: &str) -> Result<(File, Vec<String>, Vec<Column>), String> {
    let mut reader = BufReader::new(file);
    let mut line_no = 0;
    let mut keys: Vec<String> = Vec::new();
    let mut columns: Vec<Column> = Vec::new();
    let mut seen = HashSet::new();
    let mut rows = 0;
    let mut name_bytes = 0;
    while let Some(line) = read_json_line(&mut reader, &mut line_no)? {
        if line.trim().is_empty() { continue; }
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| format!("invalid JSON on line {line_no}: {e}"))?;
        let object = value.as_object().ok_or_else(|| format!("line {line_no} is not a json object"))?;
        for (key, item) in object {
            let observed = json_value_type(item, key).map_err(|e| format!("line {line_no}: {e}"))?;
            let scale = if observed == ColType::Datetime { datetime_scale(item.as_str().unwrap_or("")) } else { 0 };
            if let Some(position) = keys.iter().position(|existing| existing == key) {
                let column = &mut columns[position];
                column.ty = widen(column.ty, observed);
                column.nullable |= item.is_null();
                column.scale = column.scale.max(scale);
            } else {
                name_bytes += key.len();
                if keys.len() >= MAX_COLUMNS || name_bytes > MAX_ROW_BYTES {
                    return Err(format!("JSON schema exceeds limits on line {line_no}"));
                }
                columns.push(Column { name: unique_name(&mut seen, key, keys.len()), ty: observed,
                                      nullable: rows > 0 || item.is_null(), scale });
                keys.push(key.clone());
            }
        }
        for (i, key) in keys.iter().enumerate() { columns[i].nullable |= !object.contains_key(key); }
        rows += 1;
    }
    let mut file = reader.into_inner();
    file.rewind().map_err(|e| format!("cannot rewind '{path}': {e}"))?;
    Ok((file, keys, columns))
}

impl JsonReader {
    fn open(file: File, path: &str) -> Result<(JsonReader, Vec<Column>), String> {
        let (file, keys, columns) = json_columns(file, path)?;
        Ok((JsonReader { reader: BufReader::new(file), keys, line_no: 0, finished: false }, columns))
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<(Vec<Vec<Option<String>>>, bool), String> {
        let mut rows = Vec::new();
        let mut bytes = 0;
        while rows.len() < max_rows && !self.finished && bytes < MAX_BATCH_BYTES - MAX_ROW_BYTES {
            let Some(line) = read_json_line(&mut self.reader, &mut self.line_no)? else { self.finished = true; break; };
            if line.trim().is_empty() { continue; }
            let value: serde_json::Value = serde_json::from_str(&line)
                .map_err(|e| format!("invalid JSON on line {}: {e}", self.line_no))?;
            let object = value.as_object().ok_or_else(|| format!("line {} is not a json object", self.line_no))?;
            let row: Vec<Option<String>> = self.keys.iter().map(|key| match object.get(key) {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(text)) => Some(text.clone()),
                Some(other) => Some(other.to_string()),
            }).collect();
            bytes += row_bytes(&row)?;
            rows.push(row);
        }
        Ok((rows, self.finished))
    }
}

fn date_to_string(days: i32) -> String {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
        .map(|date| date.to_string()).unwrap_or_else(|| days.to_string())
}

fn timestamp_to_datetime(value: i64, units: i64) -> String {
    chrono::DateTime::from_timestamp(value.div_euclid(units), (value.rem_euclid(units) * (1_000_000_000 / units)) as u32)
        .map(|date| date.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        .unwrap_or_else(|| value.to_string())
}

fn field_to_text(field: &Field) -> Option<String> {
    match field {
        Field::Null => None,
        Field::Bool(value) => Some(value.to_string()),
        Field::Byte(value) => Some(value.to_string()),
        Field::Short(value) => Some(value.to_string()),
        Field::Int(value) => Some(value.to_string()),
        Field::Long(value) => Some(value.to_string()),
        Field::UByte(value) => Some(value.to_string()),
        Field::UShort(value) => Some(value.to_string()),
        Field::UInt(value) => Some(value.to_string()),
        Field::ULong(value) => Some(value.to_string()),
        Field::Float(value) => Some(value.to_string()),
        Field::Double(value) => Some(value.to_string()),
        Field::Decimal(_) => Some(field.to_string()),
        Field::Str(value) => Some(value.clone()),
        Field::Bytes(value) => Some(String::from_utf8_lossy(value.data()).to_string()),
        Field::Date(value) => Some(date_to_string(*value)),
        Field::TimestampMillis(value) => Some(timestamp_to_datetime(*value, 1000)),
        Field::TimestampMicros(value) => Some(timestamp_to_datetime(*value, 1_000_000)),
        _ => None,
    }
}

pub struct ParquetReader {
    rows: Option<RowIter<'static>>,
    indices: Vec<usize>,
    column_count: usize,
    remaining: i64,
}

fn parquet_column_type(field_type: &parquet::schema::types::Type) -> Result<ColType, String> {
    if field_type.is_group() {
        return Err(format!(
            "nested parquet columns are not supported ('{}')",
            field_type.name()
        ));
    }
    let converted = field_type.get_basic_info().converted_type();
    if converted == ConvertedType::DECIMAL { return Ok(ColType::Double); }
    if converted == ConvertedType::UINT_64 { return Ok(ColType::Varchar); }
    let ty = match field_type.get_physical_type() {
        PhysicalType::BOOLEAN => ColType::Bool,
        PhysicalType::INT32 => {
            if converted == ConvertedType::DATE {
                ColType::Date
            } else {
                ColType::Int
            }
        }
        PhysicalType::INT64 => {
            if converted == ConvertedType::TIMESTAMP_MILLIS
                || converted == ConvertedType::TIMESTAMP_MICROS
            {
                ColType::Datetime
            } else {
                ColType::Int
            }
        }
        PhysicalType::INT96 => ColType::Datetime,
        PhysicalType::FLOAT | PhysicalType::DOUBLE => ColType::Double,
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => ColType::Varchar,
    };
    Ok(ty)
}

impl ParquetReader {
    fn open(file: File, path: &str, projection: Option<&[usize]>) -> Result<(ParquetReader, Vec<Column>), String> {
        let reader = SerializedFileReader::new(file)
            .map_err(|e| format!("cannot read parquet metadata of '{path}': {e}"))?;
        let metadata = reader.metadata();
        let schema = metadata.file_metadata().schema_descr().root_schema();
        if schema.get_fields().len() > MAX_COLUMNS { return Err("Parquet schema exceeds column limit".into()); }
        let mut columns = Vec::new();
        for field in schema.get_fields() {
            let ty = parquet_column_type(field)?;
            let scale = if ty == ColType::Datetime {
                if field.get_basic_info().converted_type() == ConvertedType::TIMESTAMP_MILLIS { 3 } else { 6 }
            } else { 0 };
            columns.push(Column { name: field.name().to_string(), ty,
                nullable: field.get_basic_info().repetition() != Repetition::REQUIRED, scale });
        }
        let mut indices: Vec<usize> = projection.map_or_else(|| (0..columns.len()).collect(), |ids| ids.to_vec());
        indices.sort_unstable();
        indices.dedup();
        let fields = indices.iter().map(|&index| schema.get_fields().get(index).cloned()
            .ok_or_else(|| "Parquet projection is outside the schema".to_string())).collect::<Result<Vec<_>, _>>()?;
        let projected = parquet::schema::types::Type::group_type_builder(schema.name()).with_fields(fields).build()
            .map_err(|e| format!("invalid Parquet projection: {e}"))?;
        let remaining = metadata.file_metadata().num_rows();
        let column_count = columns.len();
        let rows = if indices.is_empty() { None } else {
            Some(RowIter::from_file_into(Box::new(reader)).project(Some(projected))
                 .map_err(|e| format!("cannot project Parquet columns: {e}"))?)
        };
        Ok((ParquetReader { rows, indices, column_count, remaining }, columns))
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<(Vec<Vec<Option<String>>>, bool), String> {
        let mut rows = Vec::new();
        let mut bytes = 0;
        while rows.len() < max_rows && self.remaining > 0 && bytes < MAX_BATCH_BYTES - MAX_ROW_BYTES {
            let mut row = vec![None; self.column_count];
            if let Some(iterator) = &mut self.rows {
                let record = iterator.next().ok_or_else(|| "Parquet ended before its declared row count".to_string())?
                    .map_err(|e| format!("cannot decode parquet row: {e}"))?;
                for (&index, (_, field)) in self.indices.iter().zip(record.get_column_iter()) { row[index] = field_to_text(field); }
            }
            bytes += row_bytes(&row)?;
            rows.push(row);
            self.remaining -= 1;
        }
        Ok((rows, self.remaining == 0))
    }
}

enum Source {
    Csv(CsvReader),
    Json(JsonReader),
    Parquet(ParquetReader),
}

pub struct Reader {
    source: Source,
    state: FileState,
}

impl Reader {
    pub fn open(path: &str, format: u32, options: &Options, expected_version: &str,
                projection: Option<&[usize]>) -> Result<(Reader, Vec<Column>), String> {
        let file = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let metadata = file.metadata().map_err(|e| format!("cannot inspect '{path}': {e}"))?;
        if !metadata.is_file() { return Err(format!("not a regular file: '{path}'")); }
        let version = file_version(&metadata);
        if !expected_version.is_empty() && version != expected_version {
            return Err(format!("file changed since schema inference: '{path}'"));
        }
        let state = FileState { file: file.try_clone().map_err(|e| e.to_string())?, path: PathBuf::from(path), version };
        state.check()?;
        let (source, columns) = match format {
            FORMAT_CSV => { let (reader, columns) = CsvReader::open(file, path, options)?; (Source::Csv(reader), columns) }
            FORMAT_JSON => { let (reader, columns) = JsonReader::open(file, path)?; (Source::Json(reader), columns) }
            FORMAT_PARQUET => { let (reader, columns) = ParquetReader::open(file, path, projection)?; (Source::Parquet(reader), columns) }
            other => return Err(format!("unsupported file format code {other}")),
        };
        state.check()?;
        Ok((Reader { source, state }, columns))
    }

    pub fn version(&self) -> &str { &self.state.version }

    pub fn next_batch(&mut self, max_rows: usize) -> Result<(Vec<Vec<Option<String>>>, bool), String> {
        self.state.check()?;
        let max_rows = if max_rows == 0 { 1024 } else { max_rows.min(1024) };
        let batch = match &mut self.source {
            Source::Csv(reader) => reader.next_batch(max_rows),
            Source::Json(reader) => reader.next_batch(max_rows),
            Source::Parquet(reader) => reader.next_batch(max_rows),
        }?;
        self.state.check()?;
        Ok(batch)
    }
}

pub struct CsvWriter {
    writer: Option<csv::Writer<NamedTempFile>>,
    target: PathBuf,
    nullstr: String,
    header: bool,
}

impl CsvWriter {
    pub fn open(path: &str, options: &Options) -> Result<CsvWriter, String> {
        match std::fs::symlink_metadata(path) {
            Ok(_) => return Err(format!("COPY target already exists: '{path}'")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
            Err(e) => return Err(format!("cannot inspect COPY target '{path}': {e}")),
        }
        let target = PathBuf::from(path);
        let temporary = tempfile::Builder::new().prefix(".seekdb-copy-")
            .tempfile_in(target.parent().unwrap_or(Path::new(".")))
            .map_err(|e| format!("cannot create temporary COPY file: {e}"))?;
        let writer = csv::WriterBuilder::new().delimiter(options.delimiter).quote(options.quote)
            .escape(options.escape).double_quote(options.escape == options.quote).flexible(true).from_writer(temporary);
        Ok(CsvWriter { writer: Some(writer), target, nullstr: options.nullstr.clone(), header: options.header })
    }

    pub fn push(&mut self, rows: &[Vec<Option<String>>], header: Option<&[String]>) -> Result<(), String> {
        let writer = self.writer.as_mut().ok_or_else(|| "COPY writer is closed".to_string())?;
        if let Some(header) = header.filter(|_| self.header) {
            writer.write_record(header).map_err(|e| format!("cannot write CSV header: {e}"))?;
            self.header = false;
        }
        for row in rows {
            writer.write_record(row.iter().map(|cell| cell.as_deref().unwrap_or(&self.nullstr)))
                .map_err(|e| format!("cannot write CSV record: {e}"))?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), String> {
        let mut writer = self.writer.take().ok_or_else(|| "COPY writer is closed".to_string())?;
        writer.flush().map_err(|e| format!("cannot flush CSV file: {e}"))?;
        let temporary = writer.into_inner().map_err(|e| format!("cannot finish CSV file: {e}"))?;
        temporary.persist_noclobber(&self.target).map_err(|e| format!("cannot commit COPY target '{}': {e}", self.target.display()))?;
        Ok(())
    }
}
