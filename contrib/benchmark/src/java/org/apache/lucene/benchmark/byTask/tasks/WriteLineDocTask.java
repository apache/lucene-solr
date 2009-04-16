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

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.BasicDocMaker;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * A task which writes documents, one line per document. Each line is in the
 * following format: title &lt;TAB&gt; date &lt;TAB&gt; body. The output of this
 * taske can be consumed by
 * {@link org.apache.lucene.benchmark.byTask.feeds.LineDocMaker} and is intended
 * to save the IO overhead of opening a file per doument to be indexed.<br>
 * 
 * Supports the following parameters:
 * <ul>
 * <li>line.file.out - the name of the file to write the output to. That
 * parameter is mandatory. <b>NOTE:</b> the file is re-created.
 * <li>bzip.compression - whether the output should be bzip-compressed. This is
 * recommended when the output file is expected to be large. (optional, default:
 * false).
 * <li>doc.writeline.log.step - controls how many records to process before
 * logging the status of the task. <b>NOTE:</b> to disable logging, set this
 * value to 0 or negative. (optional, default:1000).
 * </ul>
 */
public class WriteLineDocTask extends PerfTask {

  /**
   * Default value for property <code>doc.add.log.step<code> - indicating how often 
   * an "added N docs" message should be logged.  
   */
  public static final int DEFAULT_WRITELINE_DOC_LOG_STEP = 1000;
  public final static char SEP = '\t';

  private int logStep = -1;
  private int docSize = 0;
  int count = 0;
  private BufferedWriter lineFileOut = null;
  private DocMaker docMaker;
  
  public WriteLineDocTask(PerfRunData runData) throws Exception {
    super(runData);
    Config config = runData.getConfig();
    String fileName = config.get("line.file.out", null);
    if (fileName == null) {
      throw new IllegalArgumentException("line.file.out must be set");
    }

    OutputStream out = new FileOutputStream(fileName);
    boolean doBzipCompression = false;
    String doBZCompress = config.get("bzip.compression", null);
    if (doBZCompress != null) {
      // Property was set, use the value.
      doBzipCompression = Boolean.valueOf(doBZCompress).booleanValue();
    } else {
      // Property was not set, attempt to detect based on file's extension
      doBzipCompression = fileName.endsWith("bz2");
    }

    if (doBzipCompression) {
      // Wrap with BOS since BZip2CompressorOutputStream calls out.write(int) 
      // and does not use the write(byte[]) version. This proved to speed the 
      // compression process by 70% !
      out = new BufferedOutputStream(out, 1 << 16);
      out = new CompressorStreamFactory().createCompressorOutputStream("bzip2", out);
    }
    lineFileOut = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"), 1 << 16);
    docMaker = runData.getDocMaker();
    logStep = config.get("doc.writeline.log.step", DEFAULT_WRITELINE_DOC_LOG_STEP);
    // To avoid the check 'if (logStep > 0)' in log(). This effectively turns
    // logging off.
    if (logStep <= 0) {
      logStep = Integer.MAX_VALUE;
    }
  }

  public void tearDown() throws Exception {
    log(++count);
    super.tearDown();
  }

  public int doLogic() throws Exception {
    Document doc = docSize > 0 ? docMaker.makeDocument(docSize) : docMaker.makeDocument();

    Field f = doc.getField(BasicDocMaker.BODY_FIELD);
    String body = f != null ? f.stringValue().replace('\t', ' ') : null;
    
    if (body != null) {
      f = doc.getField(BasicDocMaker.TITLE_FIELD);
      String title = f != null ? f.stringValue().replace('\t', ' ') : "";
      
      f = doc.getField(BasicDocMaker.DATE_FIELD);
      String date = f != null ? f.stringValue().replace('\t', ' ') : "";
      
      lineFileOut.write(title, 0, title.length());
      lineFileOut.write(SEP);
      lineFileOut.write(date, 0, date.length());
      lineFileOut.write(SEP);
      lineFileOut.write(body, 0, body.length());
      lineFileOut.newLine();
    }
    return 1;
  }

  private void log(int count) {
    // logStep is initialized in the ctor to a positive value. If the config
    // file indicates no logging, or contains an invalid value, logStep is init
    // to Integer.MAX_VALUE, so that logging will not occur (at least for the
    // first Integer.MAX_VALUE records).
    if (count % logStep == 0) {
      System.out.println("--> " + Thread.currentThread().getName()
          + " processed (write line) " + count + " docs");
    }
  }

  public void close() throws Exception {
    lineFileOut.close();
    super.close();
  }
  
  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  public void setParams(String params) {
    if (super.supportsParams()) {
      super.setParams(params);
    }
    docSize = (int) Float.parseFloat(params); 
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  public boolean supportsParams() {
    return true;
  }
  
}
