/*
 * Copyright (c) 2025 OceanBase.
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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/object/ob_obj_cast.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_, file_column_ids_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObVarcharType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_file;
    OZ (open_file_table());
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (ObVarcharType == MY_SPEC.value_expr_->datum_meta_.type_) {
      file_reader_.close(); file_rows_.reset(); file_names_.reset();
      file_batch_allocator_.reuse(); file_allocator_.reuse();
      OZ (open_file_table());
    }
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  file_reader_.close();
  file_rows_.reset(); file_names_.reset();
  file_batch_allocator_.reuse(); file_allocator_.reuse();
  return ret;
}

void ObFunctionTableOp::destroy()
{
  file_reader_.~ObFileSqlReader();
  file_rows_.~ObArray();
  file_names_.~ObArray();
  file_batch_allocator_.~ObArenaAllocator();
  file_allocator_.~ObArenaAllocator();
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::open_file_table()
{
  using namespace share::file_sql;
  int ret = OB_SUCCESS;
  ObDatum *config = nullptr;
  ObObj enabled, secure;
  ObString expected_version;
  OZ (ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled));
  if (OB_SUCC(ret) && enabled.get_int() == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL is disabled");
  }
  OZ (ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_SECURE_FILE_PRIV, secure));
  OZ (MY_SPEC.value_expr_->eval(eval_ctx_, config));
  OZ (file_sql_parse_config(file_allocator_, config->get_string(), file_path_, file_format_, file_options_, expected_version));
  file_pos_ = 0;
  file_eof_ = false;
  if (OB_SUCC(ret) && file_format_ == FILE_SQL_FORMAT_GLOB) {
    OZ (file_sql_glob(file_allocator_, file_path_, file_names_));
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names_.count(); ++i) {
      ObString canonical;
      OZ (file_sql_check_path(file_names_.at(i), false, file_allocator_, canonical));
      OZ (ObResolverUtils::check_secure_path(secure.get_string(), canonical));
    }
  } else if (OB_SUCC(ret)) {
    ObString canonical;
    OZ (file_sql_check_path(file_path_, false, file_allocator_, canonical));
    OZ (ObResolverUtils::check_secure_path(secure.get_string(), canonical));
    OZ (file_reader_.open(file_allocator_, canonical, file_format_, file_options_,
                                expected_version, &MY_SPEC.file_column_ids_));
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.file_column_ids_.count(); ++i) {
      if (MY_SPEC.file_column_ids_.at(i) >= file_reader_.get_columns().count()) { ret = OB_SCHEMA_ERROR; }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_file()
{
  using namespace share::file_sql;
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  OZ (ctx_.check_status());
  if (OB_SUCC(ret) && file_format_ == FILE_SQL_FORMAT_GLOB) {
    if (file_pos_ >= file_names_.count()) { ret = OB_ITER_END; }
    else {
      for (int64_t i = 0; i < MY_SPEC.column_exprs_.count(); ++i) {
        ObExpr *expr = MY_SPEC.column_exprs_.at(i);
        expr->locate_datum_for_write(eval_ctx_).set_string(file_names_.at(file_pos_));
        expr->set_evaluated_projected(eval_ctx_);
      }
      ++file_pos_;
    }
  } else if (OB_SUCC(ret)) {
    if (file_pos_ >= file_rows_.count() && !file_eof_) {
      file_rows_.reset(); file_batch_allocator_.reuse(); file_pos_ = 0;
      OZ (file_reader_.next_batch(file_batch_allocator_, 1024, file_rows_, file_eof_));
    }
    if (OB_SUCC(ret) && file_pos_ >= file_rows_.count()) { ret = OB_ITER_END; }
    if (OB_SUCC(ret)) {
      const auto &row = file_rows_.at(file_pos_++);
      const ObDataTypeCastParams dtc = ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session());
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
        const int64_t index = MY_SPEC.file_column_ids_.at(i);
        ObExpr *expr = MY_SPEC.column_exprs_.at(i);
        ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
        if (index >= row.count()) { ret = OB_SCHEMA_ERROR; }
        else if (row.at(index).is_null_) { datum.set_null(); }
        else {
          const auto &cell = row.at(index);
          ObObj input, result;
          if (file_reader_.get_columns().at(index).type_ == FILE_SQL_COL_BOOLEAN) {
            input.set_tinyint(cell.value_.case_compare("true") == 0 || cell.value_ == "1");
          } else { input.set_varchar(cell.value_); input.set_collation_type(CS_TYPE_UTF8MB4_BIN); }
          ObCastCtx cast_ctx(&file_batch_allocator_, &dtc, CM_NONE, expr->obj_meta_.get_collation_type());
          OZ (ObObjCaster::to_type(expr->obj_meta_.get_type(), expr->obj_meta_.get_collation_type(), cast_ctx, input, result));
          OZ (datum.from_obj(result, expr->obj_datum_map_));
        }
        if (OB_SUCC(ret)) { expr->set_evaluated_projected(eval_ctx_); }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
