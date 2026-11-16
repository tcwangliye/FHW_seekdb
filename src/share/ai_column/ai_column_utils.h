/* Copyright (c) 2026 OceanBase. Licensed under the Apache License, Version 2.0. */
#pragma once
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
namespace common { class ObIAllocator; class ObISQLClient; }
namespace share {
namespace schema { class ObTableSchema; }

struct AiColumnDefinition
{
  AiColumnDefinition() : column_id_(0), state_column_id_(0), request_column_id_(0),
      result_spec_column_id_(0), max_retries_(2), definition_version_(0) {}
  uint64_t column_id_;
  uint64_t state_column_id_;
  uint64_t request_column_id_;
  uint64_t result_spec_column_id_;
  common::ObString column_name_;
  common::ObString expression_;
  common::ObString normalized_expr_;
  common::ObSEArray<uint64_t, 4> dependency_ids_;
  int64_t max_retries_;
  uint64_t definition_version_;
};

class AiColumnUtils
{
public:
  static bool has_ai_columns(const schema::ObTableSchema &table);
  static int get_definitions(const schema::ObTableSchema &table,
      common::ObIAllocator &allocator, common::ObIArray<AiColumnDefinition> &definitions);
  static int quote_identifier(const common::ObString &name, common::ObSqlString &sql);
};

// Internal SQL only: user SQL cannot enable this scope through a session variable.
class AiColumnInternalGuard
{
public:
  explicit AiColumnInternalGuard(bool retry = false);
  ~AiColumnInternalGuard();
  static bool active();
  static bool worker();
  static bool retry();
private:
  int saved_mode_;
};
}
}
