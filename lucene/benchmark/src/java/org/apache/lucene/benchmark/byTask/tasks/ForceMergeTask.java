package org.apache.lucene.benchmark.byTask.tasks;

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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexWriter;

/**
 * Runs forceMerge on the index.
 * <br>Other side effects: none.
 */
public class ForceMergeTask extends PerfTask {

  public ForceMergeTask(PerfRunData runData) {
    super(runData);
  }

  int maxNumSegments = -1;

  @Override
  public int doLogic() throws Exception {
    if (maxNumSegments == -1) {
      throw new IllegalStateException("required argument (maxNumSegments) was not specified");
    }
    IndexWriter iw = getRunData().getIndexWriter();
    iw.forceMerge(maxNumSegments);
    //System.out.println("forceMerge called");
    return 1;
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    maxNumSegments = Double.valueOf(params).intValue();
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
