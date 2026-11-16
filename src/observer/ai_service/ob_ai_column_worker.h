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
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace omt {

class ObAiColumnWorker final : public share::ObThreadPool
{
public:
  ObAiColumnWorker();
  ~ObAiColumnWorker() override;
  int init();
  int start() override;
  void stop() override;
  void wait() override;
  void destroy();

protected:
  void run1() override;

private:
  int claim_and_process_one_(bool &did_work);
  int publish_one_group_(bool &did_work);
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnWorker);
};

} // namespace omt
} // namespace oceanbase
