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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;

public class StartSolrServerTask extends PerfTask {
  
  private String xmx;
  private boolean log = true;
  private boolean enabled;
  
  public StartSolrServerTask(PerfRunData runData) {
    super(runData);
    Config config = runData.getConfig();
    enabled = config.get("solr.url", null) == null;
  }
  
  @Override
  public void setup() throws Exception {
    super.setup();
    this.xmx = getRunData().getConfig().get("solr.internal.server.xmx", "512M");
  }
  
  @Override
  public void tearDown() throws Exception {

  }
  
  @Override
  protected String getLogMessage(int recsCount) {
    return "started solr server";
  }
  
  @Override
  public int doLogic() throws Exception {
    if (enabled) {
      startSolrExample(xmx, log);
      return 1;
    }
    return 0;
  }
  
  private static void startSolrExample(String xmx, boolean log) throws IOException,
      InterruptedException, TimeoutException {
    List<String> cmd = new ArrayList<String>();
    cmd.add("./solr");
    cmd.add("start");
    cmd.add("-p");
    cmd.add("8901");
    cmd.add("-m");
    cmd.add(xmx);
    cmd.add("-e");
    cmd.add("schemaless");
    InitSolrBenchTask.runCmd(cmd, "../bin", log, true);
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    System.out.println("params:" + params);
    if (params.equalsIgnoreCase("log")) {
      this.log = true;
      System.out.println("------------> logging to stdout with new SolrServer");
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
  
}
