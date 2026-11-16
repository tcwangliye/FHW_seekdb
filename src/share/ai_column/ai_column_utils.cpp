/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#include "share/ai_column/ai_column_utils.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "common/json_type/ob_json_parse.h"
#include "common/json_type/ob_json_tree.h"
#include <stdio.h>

namespace oceanbase {
namespace share {
namespace {
thread_local int ai_column_internal_mode = 0;

int get_json_string(const common::ObJsonNode *node, common::ObString &value)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(node) || common::ObJsonNodeType::J_STRING != node->json_type()) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    value.assign_ptr(const_cast<char *>(node->get_data()),
                     static_cast<int32_t>(node->get_data_length()));
  }
  return ret;
}

int get_json_nonnegative_int(const common::ObJsonNode *node, uint64_t &value)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(node) || !node->is_number()) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    const int64_t signed_value = node->get_int();
    if (signed_value < 0) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      value = static_cast<uint64_t>(signed_value);
    }
  }
  return ret;
}

int get_json_object_member(const common::ObJsonNode *object, const char *key,
                           common::ObJsonNode *&value)
{
  int ret = common::OB_SUCCESS;
  common::ObIJsonBase *base_value = NULL;
  value = NULL;
  if (OB_ISNULL(object)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(object->get_object_value(common::ObString::make_string(key), base_value))) {
  } else if (OB_ISNULL(base_value)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    value = static_cast<common::ObJsonNode *>(base_value);
  }
  return ret;
}

int find_hidden_column(const schema::ObTableSchema &table,
                       const char *prefix,
                       const uint64_t result_column_id,
                       const schema::ObColumnSchemaV2 *&column)
{
  int ret = common::OB_SUCCESS;
  char name_buf[64];
  const int32_t len = snprintf(name_buf, sizeof(name_buf), "%s%llu", prefix,
                               static_cast<unsigned long long>(result_column_id));
  if (len <= 0 || len >= static_cast<int32_t>(sizeof(name_buf))) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    const common::ObString name(len, name_buf);
    column = table.get_column_schema(name);
    if (OB_ISNULL(column) || !column->is_hidden()) {
      ret = common::OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int read_spec(const schema::ObColumnSchemaV2 &spec_column,
              common::ObIAllocator &allocator,
              AiColumnDefinition &definition)
{
  int ret = common::OB_SUCCESS;
  const common::ObObj &default_value = spec_column.get_cur_default_value();
  if (default_value.is_null() || common::ObVarcharType != default_value.get_type()) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    const common::ObString json = default_value.get_string();
    common::ObJsonNode *root = NULL;
    if (json.empty()) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(common::ObJsonParser::get_tree(
            &allocator, json.ptr(), json.length(), root,
            common::ObJsonParser::JSN_STRICT_FLAG))) {
    } else if (OB_ISNULL(root) || common::ObJsonNodeType::J_OBJECT != root->json_type()) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      common::ObJsonNode *expression = NULL;
      common::ObJsonNode *normalized_expr = NULL;
      common::ObJsonNode *dependencies = NULL;
      common::ObJsonNode *max_retries = NULL;
      common::ObJsonNode *definition_version = NULL;
      uint64_t retries = 0;
      uint64_t version = 0;
      if (OB_FAIL(get_json_object_member(root, "expression", expression))) {
      } else if (OB_FAIL(get_json_object_member(root, "normalized_expr", normalized_expr))) {
      } else if (OB_FAIL(get_json_object_member(root, "dependency_ids", dependencies))) {
      } else if (OB_FAIL(get_json_object_member(root, "max_retries", max_retries))) {
      } else if (OB_FAIL(get_json_object_member(root, "definition_version", definition_version))) {
      } else if (OB_FAIL(get_json_string(expression, definition.expression_))) {
      } else if (OB_FAIL(get_json_string(normalized_expr, definition.normalized_expr_))) {
      } else if (OB_ISNULL(dependencies)
                 || common::ObJsonNodeType::J_ARRAY != dependencies->json_type()
                 || OB_ISNULL(max_retries) || OB_ISNULL(definition_version)) {
        ret = common::OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(get_json_nonnegative_int(max_retries, retries))) {
      } else if (OB_FAIL(get_json_nonnegative_int(definition_version, version))) {
      } else if (version == 0 || retries > static_cast<uint64_t>(INT64_MAX)) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        definition.max_retries_ = static_cast<int64_t>(retries);
        definition.definition_version_ = version;
        for (uint64_t i = 0; OB_SUCC(ret) && i < dependencies->element_count(); ++i) {
          common::ObJsonNode *dependency = dependencies->get_value(i);
          uint64_t dependency_id = 0;
          if (OB_FAIL(get_json_nonnegative_int(dependency, dependency_id))) {
          } else if (dependency_id == 0 || OB_FAIL(definition.dependency_ids_.push_back(dependency_id))) {
            if (OB_SUCC(ret)) {
              ret = common::OB_ERR_UNEXPECTED;
            }
          }
        }
      }
    }
  }
  return ret;
}
} // namespace

bool AiColumnUtils::has_ai_columns(const schema::ObTableSchema &table)
{
  bool found = false;
  for (schema::ObTableSchema::const_column_iterator iter = table.column_begin();
       !found && iter != table.column_end(); ++iter) {
    found = OB_NOT_NULL(*iter) && (*iter)->is_ai_column();
  }
  return found;
}

int AiColumnUtils::get_definitions(const schema::ObTableSchema &table,
                                   common::ObIAllocator &allocator,
                                   common::ObIArray<AiColumnDefinition> &definitions)
{
  int ret = common::OB_SUCCESS;
  for (schema::ObTableSchema::const_column_iterator iter = table.column_begin();
       OB_SUCC(ret) && iter != table.column_end(); ++iter) {
    const schema::ObColumnSchemaV2 *column = *iter;
    if (OB_ISNULL(column)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (column->is_ai_column()) {
      AiColumnDefinition definition;
      definition.column_id_ = column->get_column_id();
      definition.column_name_ = column->get_column_name_str();
      const schema::ObColumnSchemaV2 *state_column = NULL;
      const schema::ObColumnSchemaV2 *request_column = NULL;
      const schema::ObColumnSchemaV2 *spec_column = NULL;
      if (OB_FAIL(find_hidden_column(table, "__ai_state_", definition.column_id_, state_column))) {
      } else if (OB_FAIL(find_hidden_column(table, "__ai_request_", definition.column_id_, request_column))) {
      } else if (OB_FAIL(find_hidden_column(table, "__ai_spec_", definition.column_id_, spec_column))) {
      } else if (common::ObVarcharType != state_column->get_data_type()
                 || common::ObUInt64Type != request_column->get_data_type()
                 || common::ObVarcharType != spec_column->get_data_type()) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        definition.state_column_id_ = state_column->get_column_id();
        definition.request_column_id_ = request_column->get_column_id();
        definition.result_spec_column_id_ = spec_column->get_column_id();
        if (OB_FAIL(read_spec(*spec_column, allocator, definition))) {
        } else if (definition.expression_.empty() || definition.normalized_expr_.empty()) {
          ret = common::OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(definitions.push_back(definition))) {
        }
      }
    }
  }
  return ret;
}

int AiColumnUtils::quote_identifier(const common::ObString &name, common::ObSqlString &sql)
{
  int ret = common::OB_SUCCESS;
  if (name.empty()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sql.append("`"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
      if ('`' == name[i]) {
        ret = sql.append("``");
      } else {
        ret = sql.append(name.ptr() + i, 1);
      }
    }
    if (OB_SUCC(ret)) {
      ret = sql.append("`");
    }
  }
  return ret;
}

AiColumnInternalGuard::AiColumnInternalGuard(bool retry)
  : saved_mode_(ai_column_internal_mode)
{
  ai_column_internal_mode = retry ? 2 : 1;
}

AiColumnInternalGuard::~AiColumnInternalGuard()
{
  ai_column_internal_mode = saved_mode_;
}

bool AiColumnInternalGuard::active()
{
  return ai_column_internal_mode != 0;
}

bool AiColumnInternalGuard::worker()
{
  return ai_column_internal_mode == 1;
}

bool AiColumnInternalGuard::retry()
{
  return ai_column_internal_mode == 2;
}
} // namespace share
} // namespace oceanbase
