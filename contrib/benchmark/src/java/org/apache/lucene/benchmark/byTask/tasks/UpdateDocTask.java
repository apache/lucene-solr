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
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.BasicDocMaker;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import java.text.NumberFormat;


/**
 * Update a document, using IndexWriter.updateDocument,
 * optionally with of a certain size.
 * <br>Other side effects: none.
 * <br>Relevant properties: <code>doc.add.log.step</code>.
 * <br>Takes optional param: document size. 
 */
public class UpdateDocTask extends PerfTask {

  public UpdateDocTask(PerfRunData runData) {
    super(runData);
  }

  private int logStep = -1;
  private int docSize = 0;
  int count = 0;
  
  // volatile data passed between setup(), doLogic(), tearDown().
  private Document doc = null;
  
  /*
   *  (non-Javadoc)
   * @see PerfTask#setup()
   */
  public void setup() throws Exception {
    super.setup();
    DocMaker docMaker = getRunData().getDocMaker();
    if (docSize > 0) {
      doc = docMaker.makeDocument(docSize);
    } else {
      doc = docMaker.makeDocument();
    }
  }

  /* (non-Javadoc)
   * @see PerfTask#tearDown()
   */
  public void tearDown() throws Exception {
    log(++count);
    doc = null;
    super.tearDown();
  }

  public int doLogic() throws Exception {
    final String docID = doc.get(BasicDocMaker.ID_FIELD);
    if (docID == null) {
      throw new IllegalStateException("document must define the docid field");
    }
    getRunData().getIndexWriter().updateDocument(new Term(BasicDocMaker.ID_FIELD, docID),
                                                 doc);
    return 1;
  }

  private void log (int count) {
    if (logStep<0) {
      // init once per instance
      logStep = getRunData().getConfig().get("doc.add.log.step",AddDocTask.DEFAULT_ADD_DOC_LOG_STEP);
    }
    if (logStep>0 && (count%logStep)==0) {
      double seconds = (System.currentTimeMillis() - getRunData().getStartTimeMillis())/1000.0;
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMaximumFractionDigits(2);
      System.out.println("--> "+nf.format(seconds) + " sec: " + Thread.currentThread().getName()+" processed (update) "+count+" docs");
    }
  }

  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  public void setParams(String params) {
    super.setParams(params);
    docSize = (int) Float.parseFloat(params); 
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  public boolean supportsParams() {
    return true;
  }
  
}
