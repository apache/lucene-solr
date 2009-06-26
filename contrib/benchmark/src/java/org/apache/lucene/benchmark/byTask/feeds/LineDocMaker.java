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

import java.util.Random;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

/**
 * A DocMaker reading one line at a time as a Document from a single file. This
 * saves IO cost (over DirContentSource) of recursing through a directory and
 * opening a new file for every document. It also re-uses its Document and Field
 * instance to improve indexing speed.<br>
 * The expected format of each line is (arguments are separated by &lt;TAB&gt;):
 * <i>title, date, body</i>. If a line is read in a different format, a
 * {@link RuntimeException} will be thrown. In general, you should use this doc
 * maker with files that were created with {@link WriteLineDocTask}.<br>
 * <br>
 * Config properties:
 * <ul>
 * <li>doc.random.id.limit=N (default -1) -- create random docid in the range
 * 0..N; this is useful with UpdateDoc to test updating random documents; if
 * this is unspecified or -1, then docid is sequentially assigned
 * </ul>
 */
public class LineDocMaker extends DocMaker {

  private Random r;
  private int numDocs;

  public Document makeDocument() throws Exception {

    DocState ds = reuseFields ? getDocState() : localDocState;
    DocData dd = source.getNextDocData(ds.docData);
    Document doc = reuseFields ? ds.doc : new Document();
    doc.getFields().clear();

    Field body = ds.getField(BODY_FIELD, storeVal, bodyIndexVal, termVecVal);
    body.setValue(dd.getBody());
    doc.add(body);
    
    Field title = ds.getField(TITLE_FIELD, storeVal, indexVal, termVecVal);
    title.setValue(dd.getTitle());
    doc.add(title);
    
    Field date = ds.getField(DATE_FIELD, storeVal, indexVal, termVecVal);
    date.setValue(dd.getDate());
    doc.add(date);
    
    String docID = "doc" + (r != null ? r.nextInt(numDocs) : incrNumDocsCreated());
    Field id = ds.getField(ID_FIELD, Store.YES, Index.NOT_ANALYZED_NO_NORMS, TermVector.NO);
    id.setValue(docID);
    doc.add(id);
    
    return doc;
  }

  public Document makeDocument(int size) throws Exception {
    throw new RuntimeException("cannot change document size with LineDocMaker");
  }
  
  public void setConfig(Config config) {
    super.setConfig(config);
    source = new LineDocSource();
    source.setConfig(config);
    numDocs = config.get("doc.random.id.limit", -1);
    if (numDocs != -1) {
      r = new Random(179);
    }
  }

}
