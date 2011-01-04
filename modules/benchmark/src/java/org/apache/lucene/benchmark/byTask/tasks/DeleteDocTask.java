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
import org.apache.lucene.index.IndexReader;

/**
 * Delete a document by docid. If no docid param is supplied, deletes doc with
 * <code>id = last-deleted-doc + doc.delete.step</code>.
 */
public class DeleteDocTask extends PerfTask {

  /**
   * Gap between ids of deleted docs, applies when no docid param is provided.
   */
  public static final int DEFAULT_DOC_DELETE_STEP = 8;
  
  public DeleteDocTask(PerfRunData runData) {
    super(runData);
  }

  private int deleteStep = -1;
  private static int lastDeleted = -1;

  private int docid = -1;
  private boolean byStep = true;
  
  @Override
  public int doLogic() throws Exception {
    IndexReader r = getRunData().getIndexReader();
    r.deleteDocument(docid);
    lastDeleted = docid;
    r.decRef();
    return 1; // one work item done here
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#setup()
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    if (deleteStep<0) {
      deleteStep = getRunData().getConfig().get("doc.delete.step",DEFAULT_DOC_DELETE_STEP);
    }
    // set the docid to be deleted
    docid = (byStep ? lastDeleted + deleteStep : docid);
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "deleted " + recsCount + " docs, last deleted: " + lastDeleted;
  }
  
  /**
   * Set the params (docid only)
   * @param params docid to delete, or -1 for deleting by delete gap settings.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    docid = (int) Float.parseFloat(params);
    byStep = (docid < 0);
  }
  
  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }

}
