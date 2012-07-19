package org.apache.lucene.benchmark.byTask.tasks;

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

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.document.Document;

/**
 * Add a document, optionally of a certain size.
 * <br>Other side effects: none.
 * <br>Takes optional param: document size. 
 */
public class AddDocTask extends PerfTask {

  public AddDocTask(PerfRunData runData) {
    super(runData);
  }

  private int docSize = 0;
  
  /** 
   * volatile data passed between setup(), doLogic(), tearDown().
   * the doc is created at setup() and added at doLogic(). 
   */
  protected Document doc = null;

  @Override
  public void setup() throws Exception {
    super.setup();
    DocMaker docMaker = getRunData().getDocMaker();
    if (docSize > 0) {
      doc = docMaker.makeDocument(docSize);
    } else {
      doc = docMaker.makeDocument();
    }
  }

  @Override
  public void tearDown() throws Exception {
    doc = null;
    super.tearDown();
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return String.format(Locale.ROOT, "added %9d docs",recsCount);
  }
  
  @Override
  public int doLogic() throws Exception {
    getRunData().getIndexWriter().addDocument(doc);
    return 1;
  }

  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    docSize = (int) Float.parseFloat(params); 
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
  
}
