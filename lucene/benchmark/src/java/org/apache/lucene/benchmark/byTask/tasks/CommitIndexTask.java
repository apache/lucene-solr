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


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexWriter;

/**
 * Commits the IndexWriter.
 *
 */
public class CommitIndexTask extends PerfTask {
  Map<String,String> commitUserData;

  public CommitIndexTask(PerfRunData runData) {
    super(runData);
  }
  
  @Override
  public boolean supportsParams() {
    return true;
  }
  
  @Override
  public void setParams(String params) {
    super.setParams(params);
    commitUserData = new HashMap<>();
    commitUserData.put(OpenReaderTask.USER_DATA, params);
  }
  
  @Override
  public int doLogic() throws Exception {
    IndexWriter iw = getRunData().getIndexWriter();
    if (iw != null) {
      if (commitUserData != null) {
        iw.setLiveCommitData(commitUserData.entrySet());
      }
      iw.commit();
    }
    
    return 1;
  }
}
