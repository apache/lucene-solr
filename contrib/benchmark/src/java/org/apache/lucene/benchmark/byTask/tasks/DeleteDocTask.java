package org.apache.lucene.benchmark.byTask.tasks;

import org.apache.lucene.benchmark.byTask.PerfRunData;

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

/**
 * Delete a document by docid.
 * <br>Other side effects: none.
 * <br>Relevant properties: <code>doc.delete.step, delete.log.step</code>.
 * <br>If no docid param is supplied, deletes doc with <code>id = last-deleted-doc + doc.delete.step</code>. 
 * <br>Takes optional param: document id. 
 */
public class DeleteDocTask extends PerfTask {

  /**
   * Gap between ids of deleted docs, applies when no docid param is provided.
   */
  public static final int DEFAULT_DOC_DELETE_STEP = 8;
  
  public DeleteDocTask(PerfRunData runData) {
    super(runData);
    // Override log.step, which is read by PerfTask
    int deleteLogStep = runData.getConfig().get("delete.log.step", -1);
    if (deleteLogStep != -1) {
      logStep = deleteLogStep;
    }
  }

  private int deleteStep = -1;
  private static int lastDeleted = -1;

  private int docid = -1;
  private boolean byStep = true;
  
  public int doLogic() throws Exception {
    getRunData().getIndexReader().deleteDocument(docid);
    lastDeleted = docid;
    return 1; // one work item done here
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#setup()
   */
  public void setup() throws Exception {
    super.setup();
    if (deleteStep<0) {
      deleteStep = getRunData().getConfig().get("doc.delete.step",DEFAULT_DOC_DELETE_STEP);
    }
    // set the docid to be deleted
    docid = (byStep ? lastDeleted + deleteStep : docid);
  }

  protected String getLogMessage(int recsCount) {
    return "deleted " + recsCount + " docs, last deleted: " + lastDeleted;
  }
  
  /**
   * Set the params (docid only)
   * @param params docid to delete, or -1 for deleting by delete gap settings.
   */
  public void setParams(String params) {
    super.setParams(params);
    docid = (int) Float.parseFloat(params);
    byStep = (docid < 0);
  }
  
  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  public boolean supportsParams() {
    return true;
  }

}
