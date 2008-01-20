package org.apache.lucene.benchmark.byTask.tasks;

/**
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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexWriter;

/**
 * Optimize the index.
 * <br>Other side effects: none.
 */
public class OptimizeTask extends PerfTask {

  public OptimizeTask(PerfRunData runData) {
    super(runData);
  }

  int maxNumSegments = 1;

  public int doLogic() throws Exception {
    IndexWriter iw = getRunData().getIndexWriter();
    iw.optimize(maxNumSegments);
    //System.out.println("optimize called");
    return 1;
  }

  public void setParams(String params) {
    super.setParams(params);
    maxNumSegments = (int) Double.valueOf(params).intValue();
  }

  public boolean supportsParams() {
    return true;
  }
}
