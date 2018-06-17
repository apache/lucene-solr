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
package org.apache.solr.benchmark.byTask.tasks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;

public class StopSolrServerTask extends PerfTask {
  
  private boolean enabled;
  
  public StopSolrServerTask(PerfRunData runData) {
    super(runData);
    Config config = runData.getConfig();
    enabled = config.get("solr.url", null) == null;
  }
  
  @Override
  public void setup() throws Exception {
    super.setup();
    
  }
  
  @Override
  public void tearDown() throws Exception {

  }
  
  @Override
  protected String getLogMessage(int recsCount) {
    return "stopping solr server";
  }
  
  @Override
  public int doLogic() throws Exception {
    if (enabled) {
      stopSolrExample();
      return 1;
    }
    return 0;
  }
  
  private static void stopSolrExample() throws IOException,
      InterruptedException, TimeoutException {
    
    List<String> cmd = new ArrayList<String>();
    cmd.add("./solr");
    cmd.add("stop");
    cmd.add("-p");
    cmd.add("8901");
    System.out.println("working dir:" + new File("../bin").getCanonicalPath());
    InitSolrBenchTask.runCmd(cmd, "../bin", true, true);
  }

}
