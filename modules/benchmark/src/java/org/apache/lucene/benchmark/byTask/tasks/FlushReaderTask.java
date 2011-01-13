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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.index.IndexReader;

public class FlushReaderTask extends PerfTask {
  String userData = null;
  
  public FlushReaderTask(PerfRunData runData) {
    super(runData);
  }
  
  @Override
  public boolean supportsParams() {
    return true;
  }
  
  @Override
  public void setParams(String params) {
    super.setParams(params);
    userData = params;
  }
  
  @Override
  public int doLogic() throws IOException {
    IndexReader reader = getRunData().getIndexReader();
    if (userData != null) {
      Map<String,String> map = new HashMap<String,String>();
      map.put(OpenReaderTask.USER_DATA, userData);
      reader.flush(map);
    } else {
      reader.flush();
    }
    reader.decRef();
    return 1;
  }
}
