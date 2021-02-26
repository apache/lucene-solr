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
 * Set a performance test configuration property. A property may have a single value, or a sequence
 * of values, separated by ":". If a sequence of values is specified, each time a new round starts,
 * the next (cyclic) value is taken. <br>
 * Other side effects: none. <br>
 * Takes mandatory param: "name,value" pair.
 *
 * @see org.apache.lucene.benchmark.byTask.tasks.NewRoundTask
 */
public class SetPropTask extends PerfTask {

  public SetPropTask(PerfRunData runData) {
    super(runData);
  }

  private String name;
  private String value;

  @Override
  public int doLogic() throws Exception {
    if (name == null || value == null) {
      throw new Exception(
          getName() + " - undefined name or value: name=" + name + " value=" + value);
    }
    getRunData().getConfig().set(name, value);
    return 0;
  }

  /**
   * Set the params (property name and value).
   *
   * @param params property name and value separated by ','.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    int k = params.indexOf(",");
    name = params.substring(0, k).trim();
    value = params.substring(k + 1).trim();
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
}
