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
import org.apache.lucene.document.Document;


/**
 * Add a document, optionally with of a cetrain size.
 * Other side effects: none.
 */
public class AddDocTask extends PerfTask {

  public AddDocTask(PerfRunData runData) {
    super(runData);
  }

  private static int logStep = -1;
  private int docSize = 0;
  
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
    DocMaker docMaker = getRunData().getDocMaker();
    log(docMaker.getCount());
    doc = null;
    super.tearDown();
  }

  public int doLogic() throws Exception {
    getRunData().getIndexWriter().addDocument(doc);
    return 1;
  }

  private void log (int count) {
    if (logStep<0) {
      // avoid sync although race possible here
      logStep = getRunData().getConfig().get("doc.add.log.step",500);
    }
    if (logStep>0 && (count%logStep)==0) {
      System.out.println("--> processed "+count+" docs");
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
}
