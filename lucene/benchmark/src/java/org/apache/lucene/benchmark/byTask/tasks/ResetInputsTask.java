/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.benchmark.byTask.tasks;

import org.apache.lucene.benchmark.byTask.PerfRunData;

/**
 * Reset inputs so that the test run would behave, input wise, as if it just started. This affects
 * e.g. the generation of docs and queries.
 */
public class ResetInputsTask extends PerfTask {

  public ResetInputsTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() throws Exception {
    getRunData().resetInputs();
    return 0;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#shouldNotRecordStats()
   */
  @Override
  protected boolean shouldNotRecordStats() {
    return true;
  }
}
