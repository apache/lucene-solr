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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * A task which writes documents, one line per document. Each line is in the
 * following format: title &lt;TAB&gt; date &lt;TAB&gt; body. The output of this
 * task can be consumed by
 * {@link org.apache.lucene.benchmark.byTask.feeds.LineDocMaker} and is intended
 * to save the IO overhead of opening a file per document to be indexed.<br>
 * Supports the following parameters:
 * <ul>
 * <li>line.file.out - the name of the file to write the output to. That
 * parameter is mandatory. <b>NOTE:</b> the file is re-created.
 * <li>bzip.compression - whether the output should be bzip-compressed. This is
 * recommended when the output file is expected to be large. (optional, default:
 * false).
 * </ul>
 * <b>NOTE:</b> this class is not thread-safe and if used by multiple threads the
 * output is unspecified (as all will write to the same output file in a
 * non-synchronized way).
 */
public class WriteLineDocTask extends PerfTask {

  public final static char SEP = '\t';
  private static final Matcher NORMALIZER = Pattern.compile("[\t\r\n]+").matcher("");

  private int docSize = 0;
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
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "Wrote " + recsCount + " line docs";
  }
  
  @Override
  public int doLogic() throws Exception {
    Document doc = docSize > 0 ? docMaker.makeDocument(docSize) : docMaker.makeDocument();

    Field f = doc.getField(DocMaker.BODY_FIELD);
    String body = f != null ? NORMALIZER.reset(f.stringValue()).replaceAll(" ") : "";
    
    f = doc.getField(DocMaker.TITLE_FIELD);
    String title = f != null ? NORMALIZER.reset(f.stringValue()).replaceAll(" ") : "";
    
    if (body.length() > 0 || title.length() > 0) {
      
      f = doc.getField(DocMaker.DATE_FIELD);
      String date = f != null ? NORMALIZER.reset(f.stringValue()).replaceAll(" ") : "";
      
      lineFileOut.write(title, 0, title.length());
      lineFileOut.write(SEP);
      lineFileOut.write(date, 0, date.length());
      lineFileOut.write(SEP);
      lineFileOut.write(body, 0, body.length());
      lineFileOut.newLine();
    }
    return 1;
  }

  @Override
  public void close() throws Exception {
    lineFileOut.close();
    super.close();
  }
  
  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    if (super.supportsParams()) {
      super.setParams(params);
    }
    docSize = (int) Float.parseFloat(params); 
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
  
}
