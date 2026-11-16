/*
 * Copyright (c) 2026 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{
namespace file_sql
{

enum ObFileSqlFormat : uint32_t
{
  FILE_SQL_FORMAT_CSV = 0,
  FILE_SQL_FORMAT_JSON = 1,
  FILE_SQL_FORMAT_PARQUET = 2,
  FILE_SQL_FORMAT_GLOB = 3,
};

enum ObFileSqlColumnType : uint32_t
{
  FILE_SQL_COL_NULL = 0,
  FILE_SQL_COL_BOOLEAN = 1,
  FILE_SQL_COL_BIGINT = 2,
  FILE_SQL_COL_DOUBLE = 3,
  FILE_SQL_COL_VARCHAR = 4,
  FILE_SQL_COL_DATE = 5,
  FILE_SQL_COL_DATETIME = 6,
};

struct ObFileSqlColumn
{
  common::ObString name_;
  ObFileSqlColumnType type_;
  bool nullable_ = true;
  int16_t scale_ = 0;
  TO_STRING_KV(K_(name), K_(type));
};

struct ObFileSqlCell
{
  bool is_null_;
  common::ObString value_;
  TO_STRING_KV(K_(is_null), K_(value));
};


// The constant table expression carries these options through existing plan serialization.
int file_sql_get_column_names(common::ObIAllocator &allocator, const common::ObString &config,
                               common::ObIArray<common::ObString> &names);
int file_sql_append_json_string(common::ObSqlString &out, const common::ObString &text);
int file_sql_make_config(common::ObIAllocator &allocator, const common::ObString &path,
                         uint32_t format, const common::ObString &options,
                         common::ObString &config);
int file_sql_parse_config(common::ObIAllocator &allocator, const common::ObString &config,
                          common::ObString &path, uint32_t &format, common::ObString &options,
                          common::ObString &version);

// A file behaves as a read-only table. The schema is inferred once when the
// reader is opened and rows are streamed in batches afterwards.
class ObFileSqlReader
{
public:
  ObFileSqlReader();
  ~ObFileSqlReader();

  int open(common::ObIAllocator &allocator,
           const common::ObString &path,
           const uint32_t format,
           const common::ObString &options_json,
           const common::ObString &expected_version = common::ObString(),
           const common::ObIArray<int64_t> *projection = nullptr);
  int next_batch(common::ObIAllocator &allocator,
                 const int64_t max_rows,
                 common::ObIArray<common::ObArray<ObFileSqlCell>> &rows,
                 bool &eof);
  const common::ObIArray<ObFileSqlColumn> &get_columns() const { return columns_; }
  const common::ObString &get_version() const { return version_; }
  void close();
private:
  uint64_t handle_;
  common::ObArenaAllocator local_allocator_;
  common::ObArray<ObFileSqlColumn> columns_;
  common::ObString version_;
  DISALLOW_COPY_AND_ASSIGN(ObFileSqlReader);
};

// Expands a glob pattern into the matching regular files.
int file_sql_glob(common::ObIAllocator &allocator,
                  const common::ObString &pattern,
                  common::ObIArray<common::ObString> &files);

// Backs COPY ... TO. Publish on finish; close discards the temporary file.
class ObFileSqlCsvWriter
{
public:
  ObFileSqlCsvWriter();
  ~ObFileSqlCsvWriter();

  int open(common::ObIAllocator &allocator,
           const common::ObString &path,
           const common::ObString &options_json);
  int write(const common::ObIArray<common::ObString> &header,
            const common::ObIArray<common::ObArray<ObFileSqlCell>> &rows);
  int finish();
  void close();
private:
  uint64_t handle_;
  DISALLOW_COPY_AND_ASSIGN(ObFileSqlCsvWriter);
};

// Canonicalize an absolute path; SQL callers apply the existing secure_file_priv check.
int file_sql_check_path(const common::ObString &path,
                        const bool for_write,
                        common::ObIAllocator &allocator,
                        common::ObString &resolved_path);

} // namespace file_sql
} // namespace share
} // namespace oceanbase
