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

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.StreamUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

/**
 * A task which writes documents, one line per document. Each line is in the following format: title
 * &lt;TAB&gt; date &lt;TAB&gt; body. The output of this task can be consumed by {@link
 * org.apache.lucene.benchmark.byTask.feeds.LineDocSource} and is intended to save the IO overhead
 * of opening a file per document to be indexed.
 *
 * <p>The format of the output is set according to the output file extension. Compression is
 * recommended when the output file is expected to be large. See info on file extensions in {@link
 * org.apache.lucene.benchmark.byTask.utils.StreamUtils.Type}
 *
 * <p>Supports the following parameters:
 *
 * <ul>
 *   <li><b>line.file.out</b> - the name of the file to write the output to. That parameter is
 *       mandatory. <b>NOTE:</b> the file is re-created.
 *   <li><b>line.fields</b> - which fields should be written in each line. (optional, default:
 *       {@link #DEFAULT_FIELDS}).
 *   <li><b>sufficient.fields</b> - list of field names, separated by comma, which, if all of them
 *       are missing, the document will be skipped. For example, to require that at least one of
 *       f1,f2 is not empty, specify: "f1,f2" in this field. To specify that no field is required,
 *       i.e. that even empty docs should be emitted, specify <b>","</b>. (optional, default: {@link
 *       #DEFAULT_SUFFICIENT_FIELDS}).
 * </ul>
 *
 * <b>NOTE:</b> this class is not thread-safe and if used by multiple threads the output is
 * unspecified (as all will write to the same output file in a non-synchronized way).
 */
public class WriteLineDocTask extends PerfTask {

  public static final String FIELDS_HEADER_INDICATOR = "FIELDS_HEADER_INDICATOR###";

  public static final char SEP = '\t';

  /** Fields to be written by default */
  public static final String[] DEFAULT_FIELDS =
      new String[] {
        DocMaker.TITLE_FIELD, DocMaker.DATE_FIELD, DocMaker.BODY_FIELD,
      };

  /** Default fields which at least one of them is required to not skip the doc. */
  public static final String DEFAULT_SUFFICIENT_FIELDS =
      DocMaker.TITLE_FIELD + ',' + DocMaker.BODY_FIELD;

  private int docSize = 0;
  protected final String fname;
  private final PrintWriter lineFileOut;
  private final DocMaker docMaker;
  private final ThreadLocal<StringBuilder> threadBuffer = new ThreadLocal<>();
  private final ThreadLocal<Matcher> threadNormalizer = new ThreadLocal<>();
  private final String[] fieldsToWrite;
  private final boolean[] sufficientFields;
  private final boolean checkSufficientFields;

  public WriteLineDocTask(PerfRunData runData) throws Exception {
    super(runData);
    Config config = runData.getConfig();
    fname = config.get("line.file.out", null);
    if (fname == null) {
      throw new IllegalArgumentException("line.file.out must be set");
    }
    OutputStream out = StreamUtils.outputStream(Paths.get(fname));
    lineFileOut =
        new PrintWriter(
            new BufferedWriter(
                new OutputStreamWriter(out, StandardCharsets.UTF_8), StreamUtils.BUFFER_SIZE));
    docMaker = runData.getDocMaker();

    // init fields
    String f2r = config.get("line.fields", null);
    if (f2r == null) {
      fieldsToWrite = DEFAULT_FIELDS;
    } else {
      if (f2r.indexOf(SEP) >= 0) {
        throw new IllegalArgumentException(
            "line.fields " + f2r + " should not contain the separator char: " + SEP);
      }
      fieldsToWrite = f2r.split(",");
    }

    // init sufficient fields
    sufficientFields = new boolean[fieldsToWrite.length];
    String suff = config.get("sufficient.fields", DEFAULT_SUFFICIENT_FIELDS);
    if (",".equals(suff)) {
      checkSufficientFields = false;
    } else {
      checkSufficientFields = true;
      HashSet<String> sf = new HashSet<>(Arrays.asList(suff.split(",")));
      for (int i = 0; i < fieldsToWrite.length; i++) {
        if (sf.contains(fieldsToWrite[i])) {
          sufficientFields[i] = true;
        }
      }
    }

    writeHeader(lineFileOut);
  }

  /** Write header to the lines file - indicating how to read the file later. */
  protected void writeHeader(PrintWriter out) {
    StringBuilder sb = threadBuffer.get();
    if (sb == null) {
      sb = new StringBuilder();
      threadBuffer.set(sb);
    }
    sb.setLength(0);
    sb.append(FIELDS_HEADER_INDICATOR);
    for (String f : fieldsToWrite) {
      sb.append(SEP).append(f);
    }
    out.println(sb.toString());
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "Wrote " + recsCount + " line docs";
  }

  @Override
  public int doLogic() throws Exception {
    Document doc = docSize > 0 ? docMaker.makeDocument(docSize) : docMaker.makeDocument();

    Matcher matcher = threadNormalizer.get();
    if (matcher == null) {
      matcher = Pattern.compile("[\t\r\n]+").matcher("");
      threadNormalizer.set(matcher);
    }

    StringBuilder sb = threadBuffer.get();
    if (sb == null) {
      sb = new StringBuilder();
      threadBuffer.set(sb);
    }
    sb.setLength(0);

    boolean sufficient = !checkSufficientFields;
    for (int i = 0; i < fieldsToWrite.length; i++) {
      IndexableField f = doc.getField(fieldsToWrite[i]);
      String text = f == null ? "" : matcher.reset(f.stringValue()).replaceAll(" ").trim();
      sb.append(text).append(SEP);
      sufficient |= text.length() > 0 && sufficientFields[i];
    }
    if (sufficient) {
      sb.setLength(sb.length() - 1); // remove redundant last separator
      // lineFileOut is a PrintWriter, which synchronizes internally in println.
      lineFileOut(doc).println(sb.toString());
    }

    return 1;
  }

  /** Selects output line file by written doc. Default: original output line file. */
  protected PrintWriter lineFileOut(Document doc) {
    return lineFileOut;
  }

  @Override
  public void close() throws Exception {
    lineFileOut.close();
    super.close();
  }

  /**
   * Set the params (docSize only)
   *
   * @param params docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    docSize = (int) Float.parseFloat(params);
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
