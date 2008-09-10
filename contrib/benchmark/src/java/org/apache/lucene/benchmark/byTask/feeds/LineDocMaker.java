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

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.tasks.WriteLineDocTask;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * A DocMaker reading one line at a time as a Document from
 * a single file.  This saves IO cost (over DirDocMaker) of
 * recursing through a directory and opening a new file for
 * every document.  It also re-uses its Document and Field
 * instance to improve indexing speed.
 *
 * Config properties:
 * docs.file=&lt;path to the file%gt;
 * doc.reuse.fields=true|false (default true)
 */
public class LineDocMaker extends BasicDocMaker {

  FileInputStream fileIS;
  BufferedReader fileIn;
  ThreadLocal docState = new ThreadLocal();
  private String fileName;

  private static int READER_BUFFER_BYTES = 64*1024;
  private final DocState localDocState = new DocState();

  private boolean doReuseFields = true;
  
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

    final static String SEP = WriteLineDocTask.SEP;

    public Document setFields(String line) {
      // title <TAB> date <TAB> body <NEWLINE>
      final String title, date, body;

      int spot = line.indexOf(SEP);
      if (spot != -1) {
        title = line.substring(0, spot);
        int spot2 = line.indexOf(SEP, 1+spot);
        if (spot2 != -1) {
          date = line.substring(1+spot, spot2);
          body = line.substring(1+spot2, line.length());
        } else 
          date = body = "";
      } else
        title = date = body = "";

      if (doReuseFields) {
        titleField.setValue(title);
        dateField.setValue(date);
        bodyField.setValue(body);
        return doc;
      } else {
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
      while(true) {
        line = fileIn.readLine();
        if (line == null) {
          // Reset the file
          openFile();
          if (!forever)
            throw new NoMoreDataException();
        } else {
          break;
        }
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
    fileName = config.get("docs.file", null);
    if (fileName == null)
      throw new RuntimeException("docs.file must be set");
    openFile();
  }

  public void setConfig(Config config) {
    super.setConfig(config);
    doReuseFields = config.get("doc.reuse.fields", true);
  }

  synchronized void openFile() {
    try {
      if (fileIn != null)
        fileIn.close();
      fileIS = new FileInputStream(fileName);
      fileIn = new BufferedReader(new InputStreamReader(fileIS,"UTF-8"), READER_BUFFER_BYTES);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int numUniqueTexts() {
    return -1;
  }
}
