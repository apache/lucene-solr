package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.byTask.tasks.WriteLineDocTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * A DocMaker reading one line at a time as a Document from a single file. This
 * saves IO cost (over DirDocMaker) of recursing through a directory and opening
 * a new file for every document. It also re-uses its Document and Field
 * instance to improve indexing speed.<br>
 * The expected format of each line is (arguments are separated by &lt;TAB&gt;):
 * <i>title, date, body</i>. If a line is read in a different format, a
 * {@link RuntimeException} will be thrown. In general, you should use this doc
 * maker with files that were created with {@link WriteLineDocTask}.<br><br>
 * 
 * Config properties:
 * <ul>
 * <li>docs.file=&lt;path to the file&gt;
 * <li>doc.reuse.fields=true|false (default true)
 * <li>bzip.compression=true|false (default false)
 * <li>doc.random.id.limit=N (default -1) -- create random docid in the range
 * 0..N; this is useful with UpdateDoc to test updating random documents; if
 * this is unspecified or -1, then docid is sequentially assigned
 * </ul>
 */
public class LineDocMaker extends BasicDocMaker {

  InputStream fileIS;
  BufferedReader fileIn;
  ThreadLocal docState = new ThreadLocal();
  private String fileName;

  private static int READER_BUFFER_BYTES = 64*1024;
  private final DocState localDocState = new DocState();

  private boolean doReuseFields = true;
  private boolean bzipCompressionEnabled = false;
  private Random r;
  private int numDocs;
  
  private CompressorStreamFactory csFactory = new CompressorStreamFactory();
  
  class DocState {
    Document doc;
    Field bodyField;
    Field titleField;
    Field dateField;
    Field idField;

    public DocState() {

      bodyField = new Field(BasicDocMaker.BODY_FIELD,
                            "",
                            storeVal,
                            Field.Index.ANALYZED,
                            termVecVal);
      titleField = new Field(BasicDocMaker.TITLE_FIELD,
                             "",
                             storeVal,
                             Field.Index.ANALYZED,
                             termVecVal);
      dateField = new Field(BasicDocMaker.DATE_FIELD,
                            "",
                            storeVal,
                            Field.Index.ANALYZED,
                            termVecVal);
      idField = new Field(BasicDocMaker.ID_FIELD, "", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);

      doc = new Document();
      doc.add(bodyField);
      doc.add(titleField);
      doc.add(dateField);
      doc.add(idField);
    }

    final static char SEP = WriteLineDocTask.SEP;

    private int numDocsCreated;
    private synchronized int incrNumDocsCreated() {
      return numDocsCreated++;
    }

    public Document setFields(String line) {
      // A line must be in the following format. If it's not, fail !
      // title <TAB> date <TAB> body <NEWLINE>
      int spot = line.indexOf(SEP);
      if (spot == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !");
      }
      int spot2 = line.indexOf(SEP, 1 + spot);
      if (spot2 == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !");
      }
      final String title = line.substring(0, spot);
      final String date = line.substring(1+spot, spot2);
      final String body = line.substring(1+spot2, line.length());
      final String docID = "doc" + (r != null ? r.nextInt(numDocs) : incrNumDocsCreated());

      if (doReuseFields) {
        idField.setValue(docID);
        titleField.setValue(title);
        dateField.setValue(date);
        bodyField.setValue(body);
        return doc;
      } else {
        Field localIDField = new Field(BasicDocMaker.ID_FIELD,
                                       docID,
                                       Field.Store.YES,
                                       Field.Index.NOT_ANALYZED_NO_NORMS);

        Field localTitleField = new Field(BasicDocMaker.TITLE_FIELD,
                                          title,
                                          storeVal,
                                          Field.Index.ANALYZED,
                                          termVecVal);
        Field localBodyField = new Field(BasicDocMaker.BODY_FIELD,
                                         body,
                                         storeVal,
                                         Field.Index.ANALYZED,
                                         termVecVal);
        Field localDateField = new Field(BasicDocMaker.BODY_FIELD,
                                         date,
                                         storeVal,
                                         Field.Index.ANALYZED,
                                         termVecVal);
        Document localDoc = new Document();
        localDoc.add(localIDField);
        localDoc.add(localBodyField);
        localDoc.add(localTitleField);
        localDoc.add(localDateField);
        return localDoc;
      }
    }
  }

  protected DocData getNextDocData() throws Exception {
    throw new RuntimeException("not implemented");
  }

  private DocState getDocState() {
    DocState ds = (DocState) docState.get();
    if (ds == null) {
      ds = new DocState();
      docState.set(ds);
    }
    return ds;
  }

  public Document makeDocument() throws Exception {

    String line;
    synchronized(this) {
      line = fileIn.readLine();
      if (line == null) {
        if (!forever) {
          throw new NoMoreDataException();
        }
        // Reset the file
        openFile();
        return makeDocument();
      }
    }

    if (doReuseFields)
      return getDocState().setFields(line);
    else
      return localDocState.setFields(line);
  }

  public Document makeDocument(int size) throws Exception {
    throw new RuntimeException("cannot change document size with LineDocMaker; please use DirDocMaker instead");
  }
  
  public synchronized void resetInputs() {
    super.resetInputs();
    openFile();
  }

  public void setConfig(Config config) {
    super.setConfig(config);
    fileName = config.get("docs.file", null);
    if (fileName == null) {
      throw new IllegalArgumentException("docs.file must be set");
    }
    doReuseFields = config.get("doc.reuse.fields", true);
    String doBZCompress = config.get("bzip.compression", null);
    if (doBZCompress != null) {
      // Property was set, use the value.
      bzipCompressionEnabled = Boolean.valueOf(doBZCompress).booleanValue();
    } else {
      // Property was not set, attempt to detect based on file's extension
      bzipCompressionEnabled = fileName.endsWith("bz2");
    }
    numDocs = config.get("doc.random.id.limit", -1);
    if (numDocs != -1) {
      r = new Random(179);
    }
  }

  synchronized void openFile() {
    try {
      if (fileIn != null) {
        fileIn.close();
      }
      fileIS = new FileInputStream(fileName);
      if (bzipCompressionEnabled) {
        // According to BZip2CompressorInputStream's code, it reads the first 
        // two file header chars ('B' and 'Z'). We only need to wrap the
        // underlying stream with a BufferedInputStream, since the code uses
        // the read() method exclusively.
        fileIS = new BufferedInputStream(fileIS, READER_BUFFER_BYTES);
        fileIS = csFactory.createCompressorInputStream("bzip2", fileIS);
      }
      // Wrap the stream with a BufferedReader for several reasons:
      // 1. We need the readLine() method.
      // 2. Even if bzip.compression is enabled, and is wrapped with
      // BufferedInputStream, wrapping with a buffer can still improve
      // performance, since the BIS buffer will be used to read from the
      // compressed stream, while the BR buffer will be used to read from the
      // uncompressed stream.
      fileIn = new BufferedReader(new InputStreamReader(fileIS, "UTF-8"), READER_BUFFER_BYTES);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (CompressorException e) {
      throw new RuntimeException(e);
    }
  }

  public int numUniqueTexts() {
    return -1;
  }

}
