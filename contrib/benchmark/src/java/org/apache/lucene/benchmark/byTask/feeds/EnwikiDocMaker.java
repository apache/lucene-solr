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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

/**
 * A {@link DocMaker} which reads the English Wikipedia dump. Uses
 * {@link EnwikiContentSource} as its content source, regardless if a different
 * content source was defined in the configuration.
 */
public class EnwikiDocMaker extends DocMaker {
  
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
    
    Field id = ds.getField(ID_FIELD, Store.YES, Index.NOT_ANALYZED_NO_NORMS, TermVector.NO);
    id.setValue(dd.getName());
    doc.add(id);
    
    return doc;
  }

  public Document makeDocument(int size) throws Exception {
    throw new RuntimeException("cannot change document size with EnwikiDocMaker");
  }

  public void setConfig(Config config) {
    super.setConfig(config);
    // Override whatever content source was set in the config
    source = new EnwikiContentSource();
    source.setConfig(config);
  }
  
}