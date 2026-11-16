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

#define USING_LOG_PREFIX SHARE
#include "share/file_sql/ob_file_sql_utils.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_common.h"
#include "common/json_type/ob_json_tree.h"
#include "file_sql_bridge.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_check_macros.h"

namespace oceanbase
{
namespace share
{
namespace file_sql
{

namespace
{

rust::Str to_rust_str(const common::ObString &text)
{
  return rust::Str(nullptr == text.ptr() ? "" : text.ptr(),
                   static_cast<size_t>(text.length()));
}

int parse_envelope(common::ObIAllocator &allocator,
                   const common::ObString &text,
                   common::ObJsonObject *&root)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *base = nullptr;
  root = nullptr;
  if (text.empty()) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
                 &allocator, text, common::ObJsonInType::JSON_TREE,
                 common::ObJsonInType::JSON_TREE, base))) {
    LOG_WARN("failed to parse the file reader response", K(ret));
  } else if (OB_ISNULL(base) || common::ObJsonNodeType::J_OBJECT != base->json_type()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("file reader response is not a json object", K(ret));
  } else {
    root = static_cast<common::ObJsonObject *>(base);
  }
  return ret;
}

int extract_text(const common::ObJsonNode *node, common::ObIAllocator &allocator, common::ObString &text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(ob_write_string(allocator,
                                     common::ObString(node->get_data_length(), node->get_data()),
                                     text))) {
  }
  return ret;
}

// A response either carries a payload or an "error" member.
int check_envelope_error(common::ObIAllocator &allocator,
                         common::ObJsonObject *root,
                         common::ObString &error_text)
{
  int ret = OB_SUCCESS;
  error_text.reset();
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const common::ObJsonNode *error_node = root->get_value("error");
    if (nullptr != error_node) {
      if (OB_FAIL(extract_text(error_node, allocator, error_text))) {
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("file reader reported an error", K(error_text));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, error_text.ptr());
      }
    }
  }
  return ret;
}

int append_json_string(common::ObSqlString &out, const common::ObString &text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out.append("\""))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < text.length(); ++i) {
    const char ch = text.ptr()[i];
    if ('"' == ch) {
      ret = out.append("\\\"");
    } else if ('\\' == ch) {
      ret = out.append("\\\\");
    } else if ('\n' == ch) {
      ret = out.append("\\n");
    } else if ('\r' == ch) {
      ret = out.append("\\r");
    } else if ('\t' == ch) {
      ret = out.append("\\t");
    } else if (static_cast<unsigned char>(ch) < 0x20) {
      ret = out.append_fmt("\\u%04x", static_cast<unsigned char>(ch));
    } else {
      ret = out.append_fmt("%c", ch);
    }
  }
  if (OB_SUCC(ret)) {
    ret = out.append("\"");
  }
  return ret;
}

int make_c_string(common::ObIAllocator &allocator, const common::ObString &text, char *&out)
{
  int ret = OB_SUCCESS;
  char *buffer = nullptr;
  out = nullptr;
  if (OB_ISNULL(buffer = static_cast<char *>(allocator.alloc(text.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (text.length() > 0) {
      MEMCPY(buffer, text.ptr(), text.length());
    }
    buffer[text.length()] = '\0';
    out = buffer;
  }
  return ret;
}

int resolve_path(common::ObIAllocator &allocator,
                 const common::ObString &path,
                 const bool for_write,
                 char *resolved,
                 const int64_t resolved_size)
{
  int ret = OB_SUCCESS;
  char *path_c = nullptr;
  if (OB_FAIL(make_c_string(allocator, path, path_c))) {
  } else if (nullptr != realpath(path_c, resolved)) {
    // already canonical
  } else if (!for_write) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("the file does not exist", K(path));
  } else {
    // The target may not exist yet: canonicalize its parent directory.
    char *slash = strrchr(path_c, '/');
    char parent[PATH_MAX] = {0};
    char base[PATH_MAX] = {0};
    if (nullptr == slash) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const int64_t dir_len = slash - path_c;
      const int64_t name_len = strlen(slash + 1);
      if (dir_len < 0 || name_len <= 0 || dir_len >= PATH_MAX || name_len >= PATH_MAX) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        if (dir_len == 0) { strcpy(parent, "/"); }
        else { MEMCPY(parent, path_c, dir_len); parent[dir_len] = '\0'; }
        MEMCPY(base, slash + 1, name_len);
        base[name_len] = '\0';
        if (nullptr == realpath(parent, resolved)) {
          ret = OB_FILE_NOT_EXIST;
          LOG_WARN("the target directory does not exist", K(path));
        } else if (static_cast<int64_t>(strlen(resolved)) + 1 + name_len >= resolved_size) {
          ret = OB_SIZE_OVERFLOW;
        } else {
          strcat(resolved, "/");
          strcat(resolved, base);
        }
      }
    }
  }
  return ret;
}

} // namespace

int file_sql_check_path(const common::ObString &path, const bool for_write,
                        common::ObIAllocator &allocator, common::ObString &resolved_path)
{
  int ret = OB_SUCCESS;
  char resolved[PATH_MAX] = {0};
  resolved_path.reset();
  if (path.empty() || '/' != path.ptr()[0]
      || nullptr != memchr(path.ptr(), '\0', path.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path must be absolute and contain no NUL");
  } else if (OB_FAIL(resolve_path(allocator, path, for_write, resolved, PATH_MAX))) {
  } else {
    ret = ob_write_string(allocator, common::ObString(strlen(resolved), resolved), resolved_path);
  }
  return ret;
}

int file_sql_append_json_string(common::ObSqlString &out, const common::ObString &text)
{
  return append_json_string(out, text);
}

int file_sql_get_column_names(common::ObIAllocator &allocator, const common::ObString &config,
                               common::ObIArray<common::ObString> &names)
{
  int ret = OB_SUCCESS;
  common::ObJsonObject *root = nullptr;
  OZ (parse_envelope(allocator, config, root));
  if (OB_SUCC(ret)) {
    const auto *node = root->get_value("columns");
    if (node == nullptr || node->json_type() != common::ObJsonNodeType::J_ARRAY) { ret = OB_INVALID_DATA; }
    else {
      const auto *columns = static_cast<const common::ObJsonArray *>(node);
      for (int64_t i = 0; OB_SUCC(ret) && i < columns->element_count(); ++i) {
        common::ObString name;
        OZ (extract_text(columns->get_value(i), allocator, name));
        OZ (names.push_back(name));
      }
    }
  }
  return ret;
}

int file_sql_make_config(common::ObIAllocator &allocator, const common::ObString &path,
                         uint32_t format, const common::ObString &options,
                         common::ObString &config)
{
  int ret = OB_SUCCESS;
  common::ObSqlString json;
  OZ (json.append("{\"path\":"));
  OZ (append_json_string(json, path));
  OZ (json.append_fmt(",\"format\":%u,\"options\":", format));
  OZ (append_json_string(json, options));
  OZ (json.append("}"));
  OZ (ob_write_string(allocator, json.string(), config));
  return ret;
}

int file_sql_parse_config(common::ObIAllocator &allocator, const common::ObString &config,
                          common::ObString &path, uint32_t &format, common::ObString &options,
                          common::ObString &version)
{
  int ret = OB_SUCCESS;
  common::ObJsonObject *root = nullptr;
  OZ (parse_envelope(allocator, config, root));
  if (OB_SUCC(ret)) {
    const auto *p = root->get_value("path");
    const auto *f = root->get_value("format");
    const auto *o = root->get_value("options");
    if (nullptr == p || nullptr == f || nullptr == o
        || p->json_type() != common::ObJsonNodeType::J_STRING
        || f->json_type() != common::ObJsonNodeType::J_INT
        || o->json_type() != common::ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_DATA;
    } else {
      format = static_cast<uint32_t>(f->get_int());
      OZ (extract_text(p, allocator, path));
      OZ (extract_text(o, allocator, options));
      OZ (extract_text(root->get_value("version"), allocator, version));
    }
  }
  return ret;
}

ObFileSqlReader::ObFileSqlReader() : handle_(0) {}

ObFileSqlReader::~ObFileSqlReader() { close(); }

void ObFileSqlReader::close()
{
  if (0 != handle_) {
    ::oceanbase::file_sql::close_reader(handle_);
    handle_ = 0;
  }
  columns_.reset();
  version_.reset();
  local_allocator_.reset();
}

int ObFileSqlReader::open(common::ObIAllocator &allocator,
                          const common::ObString &path,
                          const uint32_t format,
                          const common::ObString &options_json,
                          const common::ObString &expected_version,
                          const common::ObIArray<int64_t> *projection)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp;
  common::ObJsonObject *root = nullptr;
  common::ObString error_text;
  close();
  common::ObSqlString projection_json;
  if (nullptr == projection) {
    OZ (projection_json.append("null"));
  } else {
    OZ (projection_json.append("["));
    for (int64_t i = 0; OB_SUCC(ret) && i < projection->count(); ++i) {
      OZ (projection_json.append_fmt(i == 0 ? "%ld" : ",%ld", projection->at(i)));
    }
    OZ (projection_json.append("]"));
  }
  if (OB_FAIL(ret)) { return ret; }
  rust::String result = ::oceanbase::file_sql::open_reader(
      to_rust_str(path), format, to_rust_str(options_json), to_rust_str(expected_version), to_rust_str(projection_json.string()));
  const common::ObString text(static_cast<int32_t>(result.length()), result.data());
  if (OB_FAIL(parse_envelope(temp, text, root))) {
  } else if (OB_FAIL(check_envelope_error(temp, root, error_text))) {
  } else {
    const common::ObJsonNode *handle_node = root->get_value("handle");
    const common::ObJsonNode *columns_node = root->get_value("columns");
    if (OB_ISNULL(handle_node) || OB_ISNULL(columns_node)
        || common::ObJsonNodeType::J_INT != handle_node->json_type()
        || common::ObJsonNodeType::J_ARRAY != columns_node->json_type()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("malformed file reader response", K(ret));
    } else {
      handle_ = static_cast<uint64_t>(handle_node->get_int());
      OZ (extract_text(root->get_value("version"), local_allocator_, version_));
      const common::ObJsonArray *columns = static_cast<const common::ObJsonArray *>(columns_node);
      for (int64_t i = 0; OB_SUCC(ret) && i < columns->element_count(); ++i) {
        const common::ObJsonNode *column_node = columns->get_value(i);
        ObFileSqlColumn column;
        if (OB_ISNULL(column_node) || common::ObJsonNodeType::J_OBJECT != column_node->json_type()) {
          ret = OB_INVALID_DATA;
        } else {
          const common::ObJsonObject *column_object = static_cast<const common::ObJsonObject *>(column_node);
          const common::ObJsonNode *name_node = column_object->get_value("name");
          const common::ObJsonNode *type_node = column_object->get_value("type");
          if (OB_ISNULL(name_node) || OB_ISNULL(type_node)) {
            ret = OB_INVALID_DATA;
          } else if (OB_FAIL(ob_write_string(local_allocator_,
                                             common::ObString(name_node->get_data_length(),
                                                              name_node->get_data()),
                                             column.name_))) {
          } else {
            column.type_ = static_cast<ObFileSqlColumnType>(type_node->get_int());
            const auto *scale_node = column_object->get_value("scale");
            column.scale_ = nullptr == scale_node ? 0 : static_cast<int16_t>(scale_node->get_int());
            const auto *nullable_node = column_object->get_value("nullable");
            column.nullable_ = nullptr == nullable_node || static_cast<const common::ObJsonBoolean *>(nullable_node)->get_boolean();
            if (OB_FAIL(columns_.push_back(column))) {
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    close();
  }
  return ret;
}

int ObFileSqlReader::next_batch(common::ObIAllocator &allocator,
                                const int64_t max_rows,
                                common::ObIArray<common::ObArray<ObFileSqlCell>> &rows,
                                bool &eof)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp;
  common::ObJsonObject *root = nullptr;
  common::ObString error_text;
  eof = false;
  rows.reset();
  if (0 == handle_) {
    ret = OB_NOT_INIT;
    LOG_WARN("the file reader is not open", K(ret));
  } else {
    rust::String result = ::oceanbase::file_sql::reader_next(
        handle_, static_cast<uint32_t>(max_rows > 0 ? max_rows : 1024));
    const common::ObString text(static_cast<int32_t>(result.length()), result.data());
    if (OB_FAIL(parse_envelope(temp, text, root))) {
    } else if (OB_FAIL(check_envelope_error(temp, root, error_text))) {
    } else {
      const common::ObJsonNode *rows_node = root->get_value("rows");
      const common::ObJsonNode *eof_node = root->get_value("eof");
      if (OB_ISNULL(rows_node) || OB_ISNULL(eof_node)
          || common::ObJsonNodeType::J_ARRAY != rows_node->json_type()) {
        ret = OB_INVALID_DATA;
      } else {
        eof = static_cast<const common::ObJsonBoolean *>(eof_node)->get_boolean();
        const common::ObJsonArray *row_array = static_cast<const common::ObJsonArray *>(rows_node);
        for (int64_t i = 0; OB_SUCC(ret) && i < row_array->element_count(); ++i) {
          const common::ObJsonNode *row_node = row_array->get_value(i);
          common::ObArray<ObFileSqlCell> row;
          if (OB_ISNULL(row_node) || common::ObJsonNodeType::J_ARRAY != row_node->json_type()) {
            ret = OB_INVALID_DATA;
          } else {
            const common::ObJsonArray *cells = static_cast<const common::ObJsonArray *>(row_node);
            for (int64_t j = 0; OB_SUCC(ret) && j < cells->element_count(); ++j) {
              const common::ObJsonNode *cell_node = cells->get_value(j);
              ObFileSqlCell cell;
              cell.is_null_ = false;
              if (OB_ISNULL(cell_node)) {
                ret = OB_INVALID_DATA;
              } else if (common::ObJsonNodeType::J_NULL == cell_node->json_type()) {
                cell.is_null_ = true;
              } else if (OB_FAIL(ob_write_string(allocator,
                                                 common::ObString(cell_node->get_data_length(),
                                                                  cell_node->get_data()),
                                                 cell.value_))) {
              }
              if (OB_SUCC(ret) && OB_FAIL(row.push_back(cell))) {
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(rows.push_back(row))) {
            }
          }
        }
      }
    }
  }
  return ret;
}

int file_sql_glob(common::ObIAllocator &allocator,
                  const common::ObString &pattern,
                  common::ObIArray<common::ObString> &files)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp;
  common::ObJsonObject *root = nullptr;
  common::ObString error_text;
  files.reset();
  rust::String result = ::oceanbase::file_sql::glob_files(to_rust_str(pattern));
  const common::ObString text(static_cast<int32_t>(result.length()), result.data());
  if (OB_FAIL(parse_envelope(temp, text, root))) {
  } else if (OB_FAIL(check_envelope_error(temp, root, error_text))) {
  } else {
    const common::ObJsonNode *files_node = root->get_value("files");
    if (OB_ISNULL(files_node) || common::ObJsonNodeType::J_ARRAY != files_node->json_type()) {
      ret = OB_INVALID_DATA;
    } else {
      const common::ObJsonArray *array = static_cast<const common::ObJsonArray *>(files_node);
      for (int64_t i = 0; OB_SUCC(ret) && i < array->element_count(); ++i) {
        const common::ObJsonNode *node = array->get_value(i);
        common::ObString file;
        if (OB_ISNULL(node)) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(ob_write_string(allocator,
                                           common::ObString(node->get_data_length(), node->get_data()),
                                           file))) {
        } else if (OB_FAIL(files.push_back(file))) {
        }
      }
    }
  }
  return ret;
}

ObFileSqlCsvWriter::ObFileSqlCsvWriter() : handle_(0) {}

ObFileSqlCsvWriter::~ObFileSqlCsvWriter() { close(); }

void ObFileSqlCsvWriter::close()
{
  if (0 != handle_) {
    ::oceanbase::file_sql::close_csv_writer(handle_);
    handle_ = 0;
  }
}

int ObFileSqlCsvWriter::open(common::ObIAllocator &allocator,
                             const common::ObString &path,
                             const common::ObString &options_json)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp;
  common::ObJsonObject *root = nullptr;
  common::ObString error_text;
  close();
  rust::String result = ::oceanbase::file_sql::open_csv_writer(
      to_rust_str(path), to_rust_str(options_json));
  const common::ObString text(static_cast<int32_t>(result.length()), result.data());
  if (OB_FAIL(parse_envelope(temp, text, root))) {
  } else if (OB_FAIL(check_envelope_error(temp, root, error_text))) {
  } else {
    const common::ObJsonNode *handle_node = root->get_value("handle");
    if (OB_ISNULL(handle_node) || common::ObJsonNodeType::J_INT != handle_node->json_type()) {
      ret = OB_INVALID_DATA;
    } else {
      handle_ = static_cast<uint64_t>(handle_node->get_int());
    }
  }
  return ret;
}

int ObFileSqlCsvWriter::write(const common::ObIArray<common::ObString> &header,
                              const common::ObIArray<common::ObArray<ObFileSqlCell>> &rows)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp;
  common::ObJsonObject *root = nullptr;
  common::ObString error_text;
  common::ObSqlString header_json;
  common::ObSqlString rows_json;
  if (0 == handle_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(header_json.append("["))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < header.count(); ++i) {
      if (OB_FAIL(i > 0 ? header_json.append(",") : OB_SUCCESS)) {
      } else if (OB_FAIL(append_json_string(header_json, header.at(i)))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(header_json.append("]"))) {
  } else if (OB_FAIL(rows_json.append("["))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const common::ObArray<ObFileSqlCell> &row = rows.at(i);
      if (OB_FAIL(i > 0 ? rows_json.append(",") : OB_SUCCESS)) {
      } else if (OB_FAIL(rows_json.append("["))) {
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < row.count(); ++j) {
        const ObFileSqlCell &cell = row.at(j);
        if (OB_FAIL(j > 0 ? rows_json.append(",") : OB_SUCCESS)) {
        } else if (cell.is_null_) {
          if (OB_FAIL(rows_json.append("null"))) {
          }
        } else if (OB_FAIL(append_json_string(rows_json, cell.value_))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(rows_json.append("]"))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rows_json.append("]"))) {
  } else {
    rust::String result = ::oceanbase::file_sql::csv_writer_push(
        handle_, to_rust_str(common::ObString(header_json.length(), header_json.ptr())),
        to_rust_str(common::ObString(rows_json.length(), rows_json.ptr())));
    const common::ObString text(static_cast<int32_t>(result.length()), result.data());
    if (OB_FAIL(parse_envelope(temp, text, root))) {
    } else if (OB_FAIL(check_envelope_error(temp, root, error_text))) {
    }
  }
  return ret;
}


int ObFileSqlCsvWriter::finish()
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator temp;
  common::ObJsonObject *root = nullptr;
  common::ObString error_text;
  if (0 == handle_) {
    ret = OB_NOT_INIT;
  } else {
    rust::String result = ::oceanbase::file_sql::finish_csv_writer(handle_);
    handle_ = 0;
    const common::ObString text(static_cast<int32_t>(result.length()), result.data());
    if (OB_FAIL(parse_envelope(temp, text, root))) {
    } else if (OB_FAIL(check_envelope_error(temp, root, error_text))) {
    }
  }
  return ret;
}

} // namespace file_sql
} // namespace share
} // namespace oceanbase
