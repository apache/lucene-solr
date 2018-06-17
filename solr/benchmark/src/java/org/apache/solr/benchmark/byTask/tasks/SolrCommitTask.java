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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.solr.client.solrj.SolrClient;


public class SolrCommitTask extends PerfTask {

  private boolean softCommit;
  
  private List<Long> times = new ArrayList<Long>();

  public SolrCommitTask(PerfRunData runData) {
    super(runData);
  }


  @Override
  protected String getLogMessage(int recsCount) {
    return "commit done";
  }
  
  @Override
  public int doLogic() throws Exception {
    SolrClient solrServer = (SolrClient) getRunData().getPerfObject("solr.client");
    long t = System.currentTimeMillis();
    solrServer.commit(false, true, softCommit);
    times.add(Long.valueOf(System.currentTimeMillis() - t));
    return 1;
  }
  
  @Override
  public void close() {
    System.out.println("Reopen Times:");
    for(int i=0;i<times.size();i++) {
      System.out.print(" " + times.get(i));
    }
    System.out.println();
  }
  
  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    if (params.equals("soft")) {
      this.softCommit = true;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
  
}
