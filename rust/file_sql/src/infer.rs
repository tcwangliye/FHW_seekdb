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

//! Deterministic value classification and column type widening.
//!
//! The rules are fixed so that the same file always yields the same schema:
//! integer before double, date before datetime, and any ambiguity widens to
//! VARCHAR instead of silently losing text.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ColType {
    Null = 0,
    Bool = 1,
    Int = 2,
    Double = 3,
    Varchar = 4,
    Date = 5,
    Datetime = 6,
}

impl ColType {
    pub fn code(self) -> u32 {
        self as u32
    }
}

/// Validate an ISO date using the existing chrono dependency.
pub fn parse_date(text: &str) -> Option<(i64, i64, i64)> {
    use chrono::Datelike;
    if text.len() != 10 { return None; }
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    if date.year() < 1 { return None; }
    Some((date.year() as i64, date.month() as i64, date.day() as i64))
}

/// Validate a datetime without byte slicing or a second calendar implementation.
pub fn parse_datetime(text: &str) -> Option<()> {
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok().map(|_| ())
}

/// Classify one raw text cell against the configured null text.
pub fn classify(text: &str, nullstr: &str) -> ColType {
    if text.is_empty() || (!nullstr.is_empty() && text == nullstr) {
        return ColType::Null;
    }
    let lowered = text.to_ascii_lowercase();
    if lowered == "true" || lowered == "false" {
        return ColType::Bool;
    }
    if text.parse::<i64>().is_ok() {
        return ColType::Int;
    }
    let digits = text.strip_prefix('-').or_else(|| text.strip_prefix('+')).unwrap_or(text);
    if !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit()) { return ColType::Varchar; }
    if let Ok(value) = text.parse::<f64>() {
        if value.is_finite() {
            return ColType::Double;
        }
    }
    if parse_date(text).is_some() {
        return ColType::Date;
    }
    if parse_datetime(text).is_some() {
        return ColType::Datetime;
    }
    ColType::Varchar
}

/// Widen two observed types into the column type they can share.
pub fn widen(left: ColType, right: ColType) -> ColType {
    use ColType::*;
    match (left, right) {
        (Null, other) => other,
        (other, Null) => other,
        (a, b) if a == b => a,
        (Int, Double) | (Double, Int) => Double,
        (Date, Datetime) | (Datetime, Date) => Datetime,
        // Anything else is a conflict: keep the values as text.
        _ => Varchar,
    }
}
