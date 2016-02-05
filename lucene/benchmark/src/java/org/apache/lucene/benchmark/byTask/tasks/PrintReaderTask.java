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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

/**
 * Opens a reader and prints basic statistics.
 */
public class PrintReaderTask extends PerfTask {
  private String userData = null;
  
  public PrintReaderTask(PerfRunData runData) {
    super(runData);
  }
  
  @Override
  public void setParams(String params) {
    super.setParams(params);
    userData = params;
  }
  
  @Override
  public boolean supportsParams() {
    return true;
  }
  
  @Override
  public int doLogic() throws Exception {
    Directory dir = getRunData().getDirectory();
    IndexReader r = null;
    if (userData == null) 
      r = DirectoryReader.open(dir);
    else
      r = DirectoryReader.open(OpenReaderTask.findIndexCommit(dir, userData));
    System.out.println("--> numDocs:"+r.numDocs()+" dels:"+r.numDeletedDocs());
    r.close();
    return 1;
  }
}
