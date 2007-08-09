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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.BasicDocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;


public class WriteLineDocTask extends PerfTask {

  /**
   * Default value for property <code>doc.add.log.step<code> - indicating how often 
   * an "added N docs" message should be logged.  
   */
  public static final int DEFAULT_WRITELINE_DOC_LOG_STEP = 1000;

  public WriteLineDocTask(PerfRunData runData) {
    super(runData);
  }

  private int logStep = -1;
  private int docSize = 0;
  int count = 0;
  private BufferedWriter lineFileOut=null;
  private DocMaker docMaker;
  
  public final static String SEP = "\t";
  
  /*
   *  (non-Javadoc)
   * @see PerfTask#setup()
   */
  public void setup() throws Exception {
    super.setup();
    if (lineFileOut==null) {
      Config config = getRunData().getConfig();
      String fileName = config.get("line.file.out", null);
      if (fileName == null)
        throw new Exception("line.file.out must be set");
      lineFileOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName),"UTF-8"));
    }
    docMaker = getRunData().getDocMaker();
  }

  public void tearDown() throws Exception {
    log(++count);
    super.tearDown();
  }

  public int doLogic() throws Exception {
    Document doc;
    if (docSize > 0) {
      doc = docMaker.makeDocument(docSize);
    } else {
      doc = docMaker.makeDocument();
    }

    Field f = doc.getField(BasicDocMaker.BODY_FIELD);

    String body, title, date;
    if (f != null)
      body = f.stringValue().replace('\t', ' ');
    else
      body = null;
    
    f = doc.getField(BasicDocMaker.TITLE_FIELD);
    if (f != null)
      title = f.stringValue().replace('\t', ' ');
    else
      title = "";

    f = doc.getField(BasicDocMaker.DATE_FIELD);
    if (f != null)
      date = f.stringValue().replace('\t', ' ');
    else
      date = "";

    if (body != null) {
      lineFileOut.write(title, 0, title.length());
      lineFileOut.write(SEP);
      lineFileOut.write(date, 0, date.length());
      lineFileOut.write(SEP);
      lineFileOut.write(body, 0, body.length());
      lineFileOut.newLine();
      lineFileOut.flush();
    }
    return 1;
  }

  private void log (int count) {
    if (logStep<0) {
      // init once per instance
      logStep = getRunData().getConfig().get("doc.writeline.log.step", DEFAULT_WRITELINE_DOC_LOG_STEP);
    }
    if (logStep>0 && (count%logStep)==0) {
      System.out.println("--> "+Thread.currentThread().getName()+" processed (add) "+count+" docs");
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
