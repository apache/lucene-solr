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
 * Simply waits for the specified (via the parameter) amount of time. For example Wait(30s) waits
 * for 30 seconds. This is useful with background tasks to control how long the tasks run.
 *
 * <p>You can specify h, m, or s (hours, minutes, seconds) as the trailing time unit. No unit is
 * interpreted as seconds.
 */
public class WaitTask extends PerfTask {

  private double waitTimeSec;

  public WaitTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    if (params != null) {
      int multiplier;
      if (params.endsWith("s")) {
        multiplier = 1;
        params = params.substring(0, params.length() - 1);
      } else if (params.endsWith("m")) {
        multiplier = 60;
        params = params.substring(0, params.length() - 1);
      } else if (params.endsWith("h")) {
        multiplier = 3600;
        params = params.substring(0, params.length() - 1);
      } else {
        // Assume seconds
        multiplier = 1;
      }

      waitTimeSec = Double.parseDouble(params) * multiplier;
    } else {
      throw new IllegalArgumentException("you must specify the wait time, eg: 10.0s, 4.5m, 2h");
    }
  }

  @Override
  public int doLogic() throws Exception {
    Thread.sleep((long) (1000 * waitTimeSec));
    return 0;
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
