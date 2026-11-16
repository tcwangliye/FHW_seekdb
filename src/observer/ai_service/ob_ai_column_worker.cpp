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
#define USING_LOG_PREFIX SERVER_OMT
#include "observer/ai_service/ob_ai_column_worker.h"
#include "share/ai_column/ai_column_utils.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/object/ob_object.h"
#include "lib/random/ob_random.h"
#include "lib/container/ob_array.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace omt
{
namespace
{
const int64_t AI_COLUMN_WORKER_COUNT = 4;
const int64_t AI_COLUMN_WORKER_IDLE_US = 200 * 1000;
const int64_t AI_COLUMN_LEASE_SECONDS = 300;
const int64_t AI_COLUMN_RETRY_DELAY_SECONDS = 1;
enum AiColumnTaskState
{
  AI_COLUMN_TASK_PENDING = 1,
  AI_COLUMN_TASK_RUNNING = 2,
  AI_COLUMN_TASK_RETRY = 3,
  AI_COLUMN_TASK_SUCCEEDED = 4,
  AI_COLUMN_TASK_FAILED = 5
};

struct TaskRecord
{
  TaskRecord()
      : tenant_id_(0), table_id_(0), column_id_(0), request_id_(0), definition_version_(0),
        max_retries_(0), retry_count_(0), state_(0), owner_token_(0), column_name_(),
        rowkey_predicate_(), normalized_expr_(), result_sql_()
  {}
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  uint64_t request_id_;
  uint64_t definition_version_;
  int64_t max_retries_;
  int64_t retry_count_;
  int64_t state_;
  uint64_t owner_token_;
  ObString column_name_;
  ObString rowkey_predicate_;
  ObString normalized_expr_;
  ObString result_sql_;
};

int copy_string(ObIAllocator &allocator, const ObString &source, ObString &destination)
{
  int ret = OB_SUCCESS;
  destination.reset();
  if (source.length() > 0) {
    char *buffer = static_cast<char *>(allocator.alloc(source.length()));
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buffer, source.ptr(), source.length());
      destination.assign_ptr(buffer, source.length());
    }
  }
  return ret;
}

int append_sql_string_literal(const ObString &value, ObIAllocator &allocator, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_varchar(value);
  char *buffer = NULL;
  int64_t buffer_length = 0;
  int64_t pos = 0;
  if (OB_FAIL(obj.print_sql_literal(buffer, buffer_length, pos, allocator))) {
    LOG_WARN("failed to quote ai-column task string", KR(ret));
  } else if (OB_FAIL(sql.append(buffer, pos))) {
    LOG_WARN("failed to append quoted ai-column task string", KR(ret));
  }
  return ret;
}

int append_qualified_table(const ObString &database_name, const ObTableSchema &table, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(AiColumnUtils::quote_identifier(database_name, sql))) {
  } else if (OB_FAIL(sql.append("."))) {
  } else if (OB_FAIL(AiColumnUtils::quote_identifier(table.get_table_name_str(), sql))) {
  }
  return ret;
}

int append_column_name(const ObTableSchema &table, const uint64_t column_id, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column = table.get_column_schema(column_id);
  if (OB_ISNULL(column)) {
    ret = OB_SCHEMA_ERROR;
  } else if (OB_FAIL(AiColumnUtils::quote_identifier(column->get_column_name_str(), sql))) {
  }
  return ret;
}

int append_task_key(const TaskRecord &task, ObSqlString &sql)
{
  return sql.append_fmt("tenant_id = %lu AND table_id = %lu AND column_id = %lu AND request_id = %lu AND definition_version = %lu",
                        task.tenant_id_, task.table_id_, task.column_id_, task.request_id_, task.definition_version_);
}

int append_task_group_key(const TaskRecord &task, ObSqlString &sql)
{
  return sql.append_fmt("tenant_id = %lu AND table_id = %lu AND request_id = %lu",
                        task.tenant_id_, task.table_id_, task.request_id_);
}

int extract_task_record(ObMySQLResult &result, ObIAllocator &allocator,
                        const bool include_result_sql, TaskRecord &task)
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_FAIL(result.get_uint("tenant_id", task.tenant_id_))) {
  } else if (OB_FAIL(result.get_uint("table_id", task.table_id_))) {
  } else if (OB_FAIL(result.get_uint("column_id", task.column_id_))) {
  } else if (OB_FAIL(result.get_uint("request_id", task.request_id_))) {
  } else if (OB_FAIL(result.get_uint("definition_version", task.definition_version_))) {
  } else if (OB_FAIL(result.get_varchar("column_name", value))) {
  } else if (OB_FAIL(copy_string(allocator, value, task.column_name_))) {
  } else if (OB_FAIL(result.get_varchar("rowkey_predicate", value))) {
  } else if (OB_FAIL(copy_string(allocator, value, task.rowkey_predicate_))) {
  } else if (OB_FAIL(result.get_varchar("normalized_expr", value))) {
  } else if (OB_FAIL(copy_string(allocator, value, task.normalized_expr_))) {
  } else if (OB_FAIL(result.get_int("max_retries", task.max_retries_))) {
  } else if (OB_FAIL(result.get_int("retry_count", task.retry_count_))) {
  } else if (OB_FAIL(result.get_int("state", task.state_))) {
  } else if (include_result_sql && OB_FAIL(result.get_uint("owner_token", task.owner_token_))) {
  } else if (include_result_sql) {
    ret = result.get_varchar("result_sql", value);
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      ret = copy_string(allocator, value, task.result_sql_);
    }
  }
  return ret;
}

int generate_owner_token(uint64_t &owner_token)
{
  const int64_t random_value = ObRandom::rand(1, INT64_MAX);
  owner_token = static_cast<uint64_t>(random_value > 0 ? random_value : 1);
  return OB_SUCCESS;
}

int end_transaction(ObMySQLTransaction &trans, const bool commit, int ret)
{
  if (trans.is_started()) {
    const int end_ret = trans.end(commit);
    if (OB_SUCCESS != end_ret && OB_SUCCESS == ret) {
      ret = end_ret;
      LOG_WARN("failed to end ai-column transaction", KR(ret), K(commit));
    }
  }
  return ret;
}

int get_task_definition(const ObTableSchema &table,
                        const ObIArray<AiColumnDefinition> &definitions,
                        const uint64_t column_id,
                        const AiColumnDefinition *&definition)
{
  int ret = OB_SUCCESS;
  definition = NULL;
  for (int64_t i = 0; i < definitions.count(); ++i) {
    if (definitions.at(i).column_id_ == column_id) {
      definition = &definitions.at(i);
      break;
    }
  }
  if (OB_ISNULL(definition) || OB_ISNULL(table.get_column_schema(column_id))) {
    ret = OB_SCHEMA_ERROR;
  }
  return ret;
}

int append_target_where(const ObTableSchema &table,
                        const ObIArray<TaskRecord> &tasks,
                        const ObIArray<AiColumnDefinition> &definitions,
                        ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (tasks.count() <= 0 || tasks.at(0).rowkey_predicate_.empty()) {
    ret = OB_INVALID_DATA;
  } else {
    const ObString &predicate = tasks.at(0).rowkey_predicate_;
    for (int64_t i = 1; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObString &other = tasks.at(i).rowkey_predicate_;
      if (predicate.length() != other.length() ||
          (predicate.length() > 0 && 0 != MEMCMP(predicate.ptr(), other.ptr(), predicate.length()))) {
        ret = OB_INVALID_DATA;
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append(" WHERE ("))) {
  } else if (OB_SUCC(ret) && OB_FAIL(sql.append(tasks.at(0).rowkey_predicate_.ptr(),
                                                tasks.at(0).rowkey_predicate_.length()))) {
  } else if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
  } else if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const AiColumnDefinition *definition = NULL;
      if (OB_FAIL(get_task_definition(table, definitions, tasks.at(i).column_id_, definition))) {
      } else if (OB_FAIL(sql.append(" AND "))) {
      } else if (OB_FAIL(append_column_name(table, definition->request_column_id_, sql))) {
      } else if (OB_FAIL(sql.append_fmt(" = %lu", tasks.at(i).request_id_))) {
      }
    }
  }
  return ret;
}

int append_single_target_where(const ObTableSchema &table,
                               const TaskRecord &task,
                               const AiColumnDefinition &definition,
                               ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (task.rowkey_predicate_.empty()) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(sql.append(" WHERE ("))) {
  } else if (OB_FAIL(sql.append(task.rowkey_predicate_.ptr(), task.rowkey_predicate_.length()))) {
  } else if (OB_FAIL(sql.append(") AND "))) {
  } else if (OB_FAIL(append_column_name(table, definition.request_column_id_, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" = %lu", task.request_id_))) {
  }
  return ret;
}

int execute_task_expression(const TaskRecord &task,
                            const ObString &database_name,
                            const ObTableSchema &table,
                            const AiColumnDefinition &definition,
                            ObIAllocator &allocator,
                            ObString &result_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (task.normalized_expr_.empty()) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(sql.append("SELECT "))) {
  } else if (OB_FAIL(sql.append(task.normalized_expr_.ptr(), task.normalized_expr_.length()))) {
  } else if (OB_FAIL(sql.append(" FROM "))) {
  } else if (OB_FAIL(append_qualified_table(database_name, table, sql))) {
  } else if (OB_FAIL(sql.append(" WHERE ("))) {
  } else if (OB_FAIL(sql.append(task.rowkey_predicate_.ptr(), task.rowkey_predicate_.length()))) {
  } else if (OB_FAIL(sql.append(") AND "))) {
  } else if (OB_FAIL(append_column_name(table, definition.request_column_id_, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" = %lu LIMIT 1", task.request_id_))) {
  } else {
    ObMySQLProxy::MySQLResult res;
    ObMySQLResult *result = NULL;
    if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ITER_END == (ret = result->next())) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_SUCC(ret)) {
      ObObj value;
      if (OB_FAIL(result->get_obj(0, value, NULL, &allocator))) {
      } else {
        char *buffer = NULL;
        int64_t buffer_length = 0;
        int64_t pos = 0;
        if (OB_FAIL(value.print_sql_literal(buffer, buffer_length, pos, allocator))) {
        } else {
          result_sql.assign_ptr(buffer, pos);
        }
      }
    }
  }
  return ret;
}

int update_task_result(const TaskRecord &task, const uint64_t owner_token,
                       const ObString &result_sql, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("UPDATE %s SET state = %d, result_sql = ",
                             OB_ALL_AI_COLUMN_TASK_TNAME, AI_COLUMN_TASK_SUCCEEDED))) {
  } else if (OB_FAIL(append_sql_string_literal(result_sql, allocator, sql))) {
  } else if (OB_FAIL(sql.append_fmt(", retry_count = retry_count, last_error = 0, owner_token = 0, lease_until = NULL WHERE "))) {
  } else if (OB_FAIL(append_task_key(task, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND state = %d AND owner_token = %lu AND lease_until > CURRENT_TIMESTAMP(6)",
                                   AI_COLUMN_TASK_RUNNING, owner_token))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected_rows))) {
  }
  return ret;
}

int update_task_error(const TaskRecord &task, const uint64_t owner_token, const int error_code)
{
  int ret = OB_SUCCESS;
  const int64_t next_retry_count = task.retry_count_ + 1;
  const bool retry = next_retry_count <= task.max_retries_;
  const int64_t new_state = retry ? AI_COLUMN_TASK_RETRY : AI_COLUMN_TASK_FAILED;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("UPDATE %s SET state = %ld, retry_count = %ld, result_sql = NULL, last_error = %d, ",
                             OB_ALL_AI_COLUMN_TASK_TNAME, new_state, next_retry_count, error_code))) {
  } else if (retry && OB_FAIL(sql.append_fmt("eligible_ts = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %ld SECOND), ",
                                              AI_COLUMN_RETRY_DELAY_SECONDS))) {
  } else if (!retry && OB_FAIL(sql.append("eligible_ts = eligible_ts, "))) {
  } else if (OB_FAIL(sql.append("owner_token = 0, lease_until = NULL WHERE "))) {
  } else if (OB_FAIL(append_task_key(task, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND state = %d AND owner_token = %lu AND lease_until > CURRENT_TIMESTAMP(6)",
                                   AI_COLUMN_TASK_RUNNING, owner_token))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected_rows))) {
  }
  return ret;
}

int release_task(const TaskRecord &task, const uint64_t owner_token)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("UPDATE %s SET state = %d, owner_token = 0, lease_until = NULL, eligible_ts = CURRENT_TIMESTAMP(6) WHERE ",
                             OB_ALL_AI_COLUMN_TASK_TNAME, AI_COLUMN_TASK_PENDING))) {
  } else if (OB_FAIL(append_task_key(task, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND state = %d AND owner_token = %lu AND lease_until > CURRENT_TIMESTAMP(6)",
                                   AI_COLUMN_TASK_RUNNING, owner_token))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected_rows))) {
  }
  return ret;
}

int delete_task_group(const TaskRecord &task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE ", OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else if (OB_FAIL(append_task_group_key(task, sql))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected_rows))) {
  }
  return ret;
}

int delete_claimed_task(const TaskRecord &task, const uint64_t owner_token)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE ", OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else if (OB_FAIL(append_task_key(task, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND owner_token = %lu AND lease_until > CURRENT_TIMESTAMP(6)", owner_token))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected_rows))) {
  }
  return ret;
}

int claim_task(TaskRecord &task, ObIAllocator &allocator, bool &claimed)
{
  int ret = OB_SUCCESS;
  claimed = false;
  ObSqlString select_sql;
  if (OB_FAIL(select_sql.append_fmt(
          "SELECT tenant_id, table_id, column_id, request_id, definition_version, column_name, "
          "rowkey_predicate, normalized_expr, max_retries, retry_count, state FROM %s "
          "WHERE (((state = %d OR state = %d) AND eligible_ts <= CURRENT_TIMESTAMP(6)) "
          "OR (state = %d AND lease_until < CURRENT_TIMESTAMP(6))) "
          "AND (owner_token = 0 OR lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP(6)) "
          "ORDER BY eligible_ts LIMIT 1",
          OB_ALL_AI_COLUMN_TASK_TNAME, AI_COLUMN_TASK_PENDING, AI_COLUMN_TASK_RETRY,
          AI_COLUMN_TASK_RUNNING))) {
  } else {
    ObMySQLProxy::MySQLResult res;
    ObMySQLResult *result = NULL;
    if (OB_FAIL(GCTX.sql_proxy_->read(res, select_sql.ptr()))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ITER_END == (ret = result->next())) {
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      if (OB_FAIL(extract_task_record(*result, allocator, false, task))) {
      } else {
        uint64_t owner_token = 0;
        if (OB_FAIL(generate_owner_token(owner_token))) {
        } else {
          ObSqlString update_sql;
          int64_t affected_rows = 0;
          if (OB_FAIL(update_sql.append_fmt(
                  "UPDATE %s SET state = %d, owner_token = %lu, "
                  "lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %ld SECOND) WHERE ",
                  OB_ALL_AI_COLUMN_TASK_TNAME, AI_COLUMN_TASK_RUNNING, owner_token,
                  AI_COLUMN_LEASE_SECONDS))) {
          } else if (OB_FAIL(append_task_key(task, update_sql))) {
          } else if (OB_FAIL(update_sql.append_fmt(
                             " AND retry_count = %ld "
                             "AND (((state = %d OR state = %d) AND eligible_ts <= CURRENT_TIMESTAMP(6)) "
                             "OR (state = %d AND lease_until < CURRENT_TIMESTAMP(6))) "
                             "AND (owner_token = 0 OR lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP(6))",
                             task.retry_count_, AI_COLUMN_TASK_PENDING, AI_COLUMN_TASK_RETRY,
                             AI_COLUMN_TASK_RUNNING))) {
          } else if (OB_FAIL(GCTX.sql_proxy_->write(update_sql.ptr(), affected_rows))) {
          } else if (1 == affected_rows) {
            task.owner_token_ = owner_token;
            claimed = true;
          }
        }
      }
    }
  }
  return ret;
}

int append_task_select(const TaskRecord &key, ObSqlString &sql, const uint64_t owner_token)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt(
          "SELECT tenant_id, table_id, column_id, request_id, definition_version, column_name, "
          "rowkey_predicate, normalized_expr, max_retries, retry_count, state, owner_token, result_sql "
          "FROM %s WHERE ", OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else if (OB_FAIL(append_task_group_key(key, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND owner_token = %lu ORDER BY column_id", owner_token))) {
  }
  return ret;
}

int find_definition(const ObIArray<AiColumnDefinition> &definitions, const uint64_t column_id,
                    const AiColumnDefinition *&definition)
{
  int ret = OB_SUCCESS;
  definition = NULL;
  for (int64_t i = 0; i < definitions.count(); ++i) {
    if (definitions.at(i).column_id_ == column_id) {
      definition = &definitions.at(i);
      break;
    }
  }
  if (OB_ISNULL(definition)) {
    ret = OB_SCHEMA_ERROR;
  }
  return ret;
}

int update_group_retry_task(ObMySQLTransaction &trans, const TaskRecord &task,
                            const uint64_t owner_token, const int error_code)
{
  int ret = OB_SUCCESS;
  const int64_t next_retry_count = task.retry_count_ + 1;
  const bool retry = next_retry_count <= task.max_retries_;
  const int64_t new_state = retry ? AI_COLUMN_TASK_RETRY : AI_COLUMN_TASK_FAILED;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("UPDATE %s SET state = %ld, retry_count = %ld, result_sql = NULL, last_error = %d, ",
                             OB_ALL_AI_COLUMN_TASK_TNAME, new_state, next_retry_count, error_code))) {
  } else if (retry && OB_FAIL(sql.append_fmt("eligible_ts = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %ld SECOND), ",
                                              AI_COLUMN_RETRY_DELAY_SECONDS))) {
  } else if (!retry && OB_FAIL(sql.append("eligible_ts = eligible_ts, "))) {
  } else if (OB_FAIL(sql.append("owner_token = 0, lease_until = NULL WHERE "))) {
  } else if (OB_FAIL(append_task_key(task, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND state = %d AND owner_token = %lu AND lease_until > CURRENT_TIMESTAMP(6)",
                                   AI_COLUMN_TASK_SUCCEEDED, owner_token))) {
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int release_group_lease(ObMySQLTransaction &trans, const TaskRecord &key,
                        const uint64_t owner_token)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("UPDATE %s SET owner_token = 0, lease_until = NULL WHERE ",
                             OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else if (OB_FAIL(append_task_group_key(key, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND owner_token = %lu", owner_token))) {
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
  }
  return ret;
}

int delete_owned_task(ObMySQLTransaction &trans, const TaskRecord &task,
                      const uint64_t owner_token)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE ", OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else if (OB_FAIL(append_task_key(task, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND owner_token = %lu", owner_token))) {
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int delete_owned_group(ObMySQLTransaction &trans, const TaskRecord &key,
                       const uint64_t owner_token, const int64_t expected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE ", OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else if (OB_FAIL(append_task_group_key(key, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND owner_token = %lu", owner_token))) {
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
  } else if (expected_rows != affected_rows) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int append_trial_update(const ObString &database_name, const ObTableSchema &table, const TaskRecord &task,
                        const AiColumnDefinition &definition,
                        const ObIArray<TaskRecord> &group,
                        const ObIArray<AiColumnDefinition> &definitions,
                        ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("UPDATE "))) {
  } else if (OB_FAIL(append_qualified_table(database_name, table, sql))) {
  } else if (OB_FAIL(sql.append(" SET "))) {
  } else if (OB_FAIL(append_column_name(table, definition.column_id_, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" = %.*s", static_cast<int>(task.result_sql_.length()),
                                   task.result_sql_.ptr()))) {
  } else if (OB_FAIL(append_target_where(table, group, definitions, sql))) {
  }
  return ret;
}

int append_publish_update(const ObString &database_name, const ObTableSchema &table,
                          const ObIArray<TaskRecord> &tasks,
                          const ObIArray<AiColumnDefinition> &definitions,
                          ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  bool first_assignment = true;
  if (OB_FAIL(sql.append("UPDATE "))) {
  } else if (OB_FAIL(append_qualified_table(database_name, table, sql))) {
  } else if (OB_FAIL(sql.append(" SET "))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const TaskRecord &task = tasks.at(i);
      const AiColumnDefinition *definition = NULL;
      if (OB_FAIL(find_definition(definitions, task.column_id_, definition))) {
      } else {
        if (AI_COLUMN_TASK_SUCCEEDED == task.state_) {
          if (task.result_sql_.empty()) {
            ret = OB_INVALID_DATA;
          } else if (!first_assignment && OB_FAIL(sql.append(", "))) {
          } else if (OB_FAIL(append_column_name(table, definition->column_id_, sql))) {
          } else if (OB_FAIL(sql.append_fmt(" = %.*s", static_cast<int>(task.result_sql_.length()),
                                           task.result_sql_.ptr()))) {
          } else {
            first_assignment = false;
          }
        }
        if (OB_SUCC(ret)) {
          if (!first_assignment && OB_FAIL(sql.append(", "))) {
          } else if (OB_FAIL(append_column_name(table, definition->state_column_id_, sql))) {
          } else if (OB_FAIL(sql.append_fmt(" = '%s'", AI_COLUMN_TASK_SUCCEEDED == task.state_ ? "READY" : "FAILED"))) {
          } else {
            first_assignment = false;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = append_target_where(table, tasks, definitions, sql);
    }
  }
  return ret;
}

int publish_group(TaskRecord &key, ObIAllocator &allocator, bool &did_work)
{
  int ret = OB_SUCCESS;
  did_work = false;
  ObMySQLTransaction trans;
  int64_t total_count = 0;
  int64_t terminal_count = 0;
  uint64_t owner_token = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else {
    ObSqlString count_sql;
    if (OB_FAIL(count_sql.append_fmt(
            "SELECT COUNT(*) AS task_count, COUNT(CASE WHEN state IN (%d, %d) THEN 1 END) AS terminal_count "
            "FROM %s WHERE ", AI_COLUMN_TASK_SUCCEEDED, AI_COLUMN_TASK_FAILED,
            OB_ALL_AI_COLUMN_TASK_TNAME))) {
    } else if (OB_FAIL(append_task_group_key(key, count_sql))) {
    } else {
      {
        ObMySQLProxy::MySQLResult res;
        ObMySQLResult *result = NULL;
        if (OB_FAIL(trans.read(res, count_sql.ptr()))) {
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
        } else if (OB_FAIL(result->get_int("task_count", total_count))) {
        } else if (OB_FAIL(result->get_int("terminal_count", terminal_count))) {
        }
      }
      if (OB_SUCC(ret) && (total_count <= 0 || total_count != terminal_count)) {
        ret = end_transaction(trans, false, OB_SUCCESS);
      } else if (OB_SUCC(ret) && OB_FAIL(generate_owner_token(owner_token))) {
      } else if (OB_SUCC(ret)) {
        ObSqlString lease_sql;
        int64_t affected_rows = 0;
        if (OB_FAIL(lease_sql.append_fmt(
                "UPDATE %s SET owner_token = %lu, lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %ld SECOND) WHERE ",
                OB_ALL_AI_COLUMN_TASK_TNAME, owner_token, AI_COLUMN_LEASE_SECONDS))) {
        } else if (OB_FAIL(append_task_group_key(key, lease_sql))) {
        } else if (OB_FAIL(lease_sql.append_fmt(
                           " AND state IN (%d, %d) AND (owner_token = 0 OR lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP(6))",
                           AI_COLUMN_TASK_SUCCEEDED, AI_COLUMN_TASK_FAILED))) {
        } else if (OB_FAIL(trans.write(lease_sql.ptr(), affected_rows))) {
        } else if (total_count != affected_rows) {
          ret = end_transaction(trans, false, OB_SUCCESS);
        } else {
          did_work = true;
          ObArray<TaskRecord> tasks;
          ObSqlString task_sql;
          if (OB_FAIL(append_task_select(key, task_sql, owner_token))) {
          } else {
            ObMySQLProxy::MySQLResult res;
            ObMySQLResult *result = NULL;
            if (OB_FAIL(trans.read(res, task_sql.ptr()))) {
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              int next_ret = OB_SUCCESS;
              while (OB_SUCC(ret) && OB_SUCCESS == (next_ret = result->next())) {
                TaskRecord task;
                if (OB_FAIL(extract_task_record(*result, allocator, true, task))) {
                } else if (OB_FAIL(tasks.push_back(task))) {
                }
              }
              if (OB_SUCC(ret) && OB_ITER_END != next_ret) {
                ret = next_ret;
              }
            }
          }
          if (OB_SUCC(ret) && total_count != tasks.count()) {
            ret = OB_EAGAIN;
          }
          ObSchemaGetterGuard schema_guard;
          const ObTableSchema *table_schema = NULL;
          const ObDatabaseSchema *database_schema = NULL;
          ObSEArray<AiColumnDefinition, 4> definitions;
          if (OB_SUCC(ret) && OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_SUCC(ret) && OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard_with_version_in_inner_table(schema_guard))) {
          } else if (OB_SUCC(ret) && OB_FAIL(schema_guard.get_table_schema(key.table_id_, table_schema))) {
          } else if (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
          } else if (OB_SUCC(ret) && OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
          } else if (OB_SUCC(ret) && OB_ISNULL(database_schema)) {
            ret = OB_TABLE_NOT_EXIST;
          } else if (OB_SUCC(ret) && OB_FAIL(AiColumnUtils::get_definitions(*table_schema, allocator, definitions))) {
          }
          if (OB_SCHEMA_EAGAIN == ret) {
            int tmp_ret = release_group_lease(trans, key, owner_token);
            if (OB_SUCCESS != tmp_ret) {
              ret = tmp_ret;
            } else {
              ret = end_transaction(trans, true, OB_SUCCESS);
            }
          } else if (OB_TABLE_NOT_EXIST == ret) {
            int tmp_ret = delete_owned_group(trans, key, owner_token, total_count);
            ret = end_transaction(trans, OB_SUCCESS == tmp_ret, tmp_ret);
          } else if (OB_SUCC(ret)) {
            bool stale = false;
            int64_t stale_task_index = -1;
            for (int64_t i = 0; OB_SUCC(ret) && !stale && i < tasks.count(); ++i) {
              const TaskRecord &task = tasks.at(i);
              const AiColumnDefinition *definition = NULL;
              const int find_ret = find_definition(definitions, task.column_id_, definition);
              if (OB_SCHEMA_ERROR == find_ret || NULL == table_schema->get_column_schema(task.column_id_)) {
                stale = true;
                stale_task_index = i;
              } else if (OB_SUCCESS != find_ret) {
                ret = find_ret;
              } else if (definition->definition_version_ != task.definition_version_) {
                stale = true;
                stale_task_index = i;
              }
            }
            if (stale) {
              int tmp_ret = delete_owned_task(trans, tasks.at(stale_task_index), owner_token);
              if (OB_SUCCESS == tmp_ret) {
                tmp_ret = release_group_lease(trans, key, owner_token);
              }
              ret = end_transaction(trans, OB_SUCCESS == tmp_ret, tmp_ret);
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && !stale && i < tasks.count(); ++i) {
                const TaskRecord &task = tasks.at(i);
                if (AI_COLUMN_TASK_SUCCEEDED == task.state_) {
                  const AiColumnDefinition *definition = NULL;
                  if (OB_FAIL(find_definition(definitions, task.column_id_, definition))) {
                  } else {
                    int64_t affected_rows = 0;
                    if (OB_FAIL(trans.write("SAVEPOINT ai_column_validate", affected_rows))) {
                    } else {
                      ObSqlString trial_sql;
                      if (OB_FAIL(append_trial_update(database_schema->get_database_name_str(), *table_schema, task, *definition, tasks, definitions, trial_sql))) {
                      } else {
                        const int trial_ret = trans.write(trial_sql.ptr(), affected_rows);
                        if (OB_SUCCESS != trial_ret) {
                          trans.reset_last_error();
                          const int rollback_ret = trans.write("ROLLBACK TO SAVEPOINT ai_column_validate", affected_rows);
                          if (OB_SUCCESS != rollback_ret) {
                            ret = rollback_ret;
                          } else {
                            trans.reset_last_error();
                            const int retry_ret = update_group_retry_task(trans, task, owner_token, trial_ret);
                            if (OB_SUCCESS != retry_ret) {
                              ret = retry_ret;
                            } else if (OB_FAIL(release_group_lease(trans, key, owner_token))) {
                            } else {
                              ret = end_transaction(trans, true, OB_SUCCESS);
                            }
                          }
                        } else if (0 == affected_rows) {
                          if (OB_FAIL(trans.write("ROLLBACK TO SAVEPOINT ai_column_validate", affected_rows))) {
                          } else {
                            int tmp_ret = delete_owned_group(trans, key, owner_token, total_count);
                            stale = true;
                            ret = end_transaction(trans, OB_SUCCESS == tmp_ret, tmp_ret);
                          }
                        } else if (OB_FAIL(trans.write("ROLLBACK TO SAVEPOINT ai_column_validate", affected_rows))) {
                        }
                      }
                    }
                  }
                }
              }
              if (OB_SUCC(ret) && !stale && trans.is_started()) {
                ObSqlString publish_sql;
                int64_t affected_rows = 0;
                if (OB_FAIL(append_publish_update(database_schema->get_database_name_str(), *table_schema, tasks, definitions, publish_sql))) {
                } else if (OB_FAIL(trans.write(publish_sql.ptr(), affected_rows))) {
                } else if (0 == affected_rows) {
                  int tmp_ret = delete_owned_group(trans, key, owner_token, total_count);
                  ret = end_transaction(trans, OB_SUCCESS == tmp_ret, tmp_ret);
                } else if (1 != affected_rows) {
                  ret = OB_ERR_UNEXPECTED;
                } else if (OB_FAIL(delete_owned_group(trans, key, owner_token, total_count))) {
                } else {
                  ret = end_transaction(trans, true, OB_SUCCESS);
                }
              }
            }
          }
          if (OB_FAIL(ret) && trans.is_started()) {
            ret = end_transaction(trans, false, ret);
          }
        }
      }
    }
    if (OB_FAIL(ret) && trans.is_started()) {
      ret = end_transaction(trans, false, ret);
    }
  }
  return ret;
}

int find_publish_candidate(TaskRecord &key, ObIAllocator &allocator, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
          "SELECT tenant_id, table_id, column_id, request_id FROM %s "
          "WHERE state IN (%d, %d) AND (owner_token = 0 OR lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP(6)) "
          "ORDER BY eligible_ts LIMIT 1",
          OB_ALL_AI_COLUMN_TASK_TNAME, AI_COLUMN_TASK_SUCCEEDED, AI_COLUMN_TASK_FAILED))) {
  } else {
    ObMySQLProxy::MySQLResult res;
    ObMySQLResult *result = NULL;
    if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ITER_END == (ret = result->next())) {
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      if (OB_FAIL(result->get_uint("tenant_id", key.tenant_id_))) {
      } else if (OB_FAIL(result->get_uint("table_id", key.table_id_))) {
      } else if (OB_FAIL(result->get_uint("column_id", key.column_id_))) {
      } else if (OB_FAIL(result->get_uint("request_id", key.request_id_))) {
      } else {
        found = true;
      }
    }
  }
  UNUSED(allocator);
  return ret;
}

} // anonymous namespace

ObAiColumnWorker::ObAiColumnWorker()
    : share::ObThreadPool(AI_COLUMN_WORKER_COUNT), is_inited_(false)
{
}

ObAiColumnWorker::~ObAiColumnWorker()
{
  destroy();
}

int ObAiColumnWorker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(share::ObThreadPool::init())) {
  } else {
    share::ObThreadPool::set_run_wrapper(share::server_runtime());
    is_inited_ = true;
  }
  return ret;
}

int ObAiColumnWorker::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    LOG_WARN("failed to start ai-column worker threads", KR(ret));
  }
  return ret;
}

void ObAiColumnWorker::stop()
{
  if (is_inited_) {
    share::ObThreadPool::stop();
  }
}

void ObAiColumnWorker::wait()
{
  if (is_inited_) {
    share::ObThreadPool::wait();
  }
}

void ObAiColumnWorker::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    share::ObThreadPool::destroy();
    is_inited_ = false;
  }
}

void ObAiColumnWorker::run1()
{
  lib::set_thread_name("AiColumnWorker");
  AiColumnInternalGuard worker_guard(false);
  while (!has_set_stop()) {
    bool did_work = false;
    bool had_error = false;
    int ret = claim_and_process_one_(did_work);
    if (OB_FAIL(ret)) {
      had_error = true;
      LOG_WARN("ai-column task execution failed", KR(ret));
    }
    if (!did_work && !had_error) {
      ret = publish_one_group_(did_work);
      if (OB_FAIL(ret)) {
        had_error = true;
        LOG_WARN("ai-column result publication failed", KR(ret));
      }
    }
    if ((!did_work || had_error) && !has_set_stop()) {
      ob_usleep(AI_COLUMN_WORKER_IDLE_US);
    }
  }
}

int ObAiColumnWorker::claim_and_process_one_(bool &did_work)
{
  int ret = OB_SUCCESS;
  did_work = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObArenaAllocator allocator(ObMemAttr("AiColumnWorker", ObCtxIds::DEFAULT_CTX_ID));
    TaskRecord task;
    bool claimed = false;
    if (OB_FAIL(claim_task(task, allocator, claimed))) {
    } else if (claimed) {
      did_work = true;
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = NULL;
      const ObDatabaseSchema *database_schema = NULL;
      ObSEArray<AiColumnDefinition, 4> definitions;
      const AiColumnDefinition *definition = NULL;
      if (OB_ISNULL(GCTX.schema_service_)) {
        ret = release_task(task, task.owner_token_);
      } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard_with_version_in_inner_table(schema_guard))) {
        const int schema_ret = ret;
        ret = release_task(task, task.owner_token_);
        if (OB_SUCCESS == ret && OB_SCHEMA_EAGAIN != schema_ret) {
          LOG_WARN("released ai-column task while schema is unavailable", KR(schema_ret));
        }
      } else if (OB_FAIL(schema_guard.get_table_schema(task.table_id_, table_schema))) {
        const int schema_ret = ret;
        ret = OB_TABLE_NOT_EXIST == schema_ret ? delete_task_group(task)
                                               : release_task(task, task.owner_token_);
      } else if (OB_ISNULL(table_schema)) {
        ret = delete_task_group(task);
      } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
        ret = release_task(task, task.owner_token_);
      } else if (OB_ISNULL(database_schema)) {
        ret = delete_task_group(task);
      } else if (OB_FAIL(AiColumnUtils::get_definitions(*table_schema, allocator, definitions))) {
        ret = release_task(task, task.owner_token_);
      } else if (OB_FAIL(find_definition(definitions, task.column_id_, definition))) {
        ret = delete_claimed_task(task, task.owner_token_);
      } else if (definition->definition_version_ != task.definition_version_) {
        ret = delete_claimed_task(task, task.owner_token_);
      } else {
        ObString result_sql;
        ret = execute_task_expression(task, database_schema->get_database_name_str(), *table_schema, *definition, allocator, result_sql);
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = delete_claimed_task(task, task.owner_token_);
        } else if (OB_FAIL(ret)) {
          const int task_ret = ret;
          if (OB_FAIL(update_task_error(task, task.owner_token_, task_ret))) {
            LOG_WARN("failed to persist ai-column inference failure", KR(ret), K(task_ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(update_task_result(task, task.owner_token_, result_sql, allocator))) {
          LOG_WARN("failed to persist ai-column inference result", KR(ret), K(task.column_id_));
        }
      }
    }
  }
  return ret;
}

int ObAiColumnWorker::publish_one_group_(bool &did_work)
{
  int ret = OB_SUCCESS;
  did_work = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObArenaAllocator allocator(ObMemAttr("AiColPublish", ObCtxIds::DEFAULT_CTX_ID));
    TaskRecord key;
    bool found = false;
    if (OB_FAIL(find_publish_candidate(key, allocator, found))) {
    } else if (found) {
      ret = publish_group(key, allocator, did_work);
    }
  }
  return ret;
}

} // namespace omt
} // namespace oceanbase
