package org.apache.lucene.codecs.lucene40;

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

import java.util.ArrayList;
import java.util.Collections;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestLucene40PostingsReader extends LuceneTestCase {
  static final String terms[] = new String[100];
  static {
    for (int i = 0; i < terms.length; i++) {
      terms[i] = Integer.toString(i+1);
    }
  }

  /** tests terms with different probabilities of being in the document.
   *  depends heavily on term vectors cross-check at checkIndex
   */
  public void testPostings() throws Exception {
    Directory dir = newFSDirectory(_TestUtil.getTempDir("postings"));
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene40"));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    Document doc = new Document();
    
    // id field
    FieldType idType = new FieldType(StringField.TYPE_NOT_STORED);
    idType.setStoreTermVectors(true);
    Field idField = new Field("id", "", idType);
    doc.add(idField);
    
    // title field: short text field
    FieldType titleType = new FieldType(TextField.TYPE_NOT_STORED);
    titleType.setStoreTermVectors(true);
    titleType.setStoreTermVectorPositions(true);
    titleType.setStoreTermVectorOffsets(true);
    titleType.setIndexOptions(indexOptions());
    Field titleField = new Field("title", "", titleType);
    doc.add(titleField);
    
    // body field: long text field
    FieldType bodyType = new FieldType(TextField.TYPE_NOT_STORED);
    bodyType.setStoreTermVectors(true);
    bodyType.setStoreTermVectorPositions(true);
    bodyType.setStoreTermVectorOffsets(true);
    bodyType.setIndexOptions(indexOptions());
    Field bodyField = new Field("body", "", bodyType);
    doc.add(bodyField);
    
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      titleField.setStringValue(fieldValue(1));
      bodyField.setStringValue(fieldValue(3));
      iw.addDocument(doc);
      if (random().nextInt(20) == 0) {
        iw.deleteDocuments(new Term("id", Integer.toString(i)));
      }
    }
    if (random().nextBoolean()) {
      // delete 1-100% of docs
      iw.deleteDocuments(new Term("title", terms[random().nextInt(terms.length)]));
    }
    iw.close();
    dir.close(); // checkindex
  }
  
  IndexOptions indexOptions() {
    switch(random().nextInt(4)) {
      case 0: return IndexOptions.DOCS_ONLY;
      case 1: return IndexOptions.DOCS_AND_FREQS;
      case 2: return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
      default: return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    }
  }
  
  String fieldValue(int maxTF) {
    ArrayList<String> shuffled = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    int i = random().nextInt(terms.length);
    while (i < terms.length) {
      int tf =  _TestUtil.nextInt(random(), 1, maxTF);
      for (int j = 0; j < tf; j++) {
        shuffled.add(terms[i]);
      }
      i++;
    }
    Collections.shuffle(shuffled, random());
    for (String term : shuffled) {
      sb.append(term);
      sb.append(' ');
    }
    return sb.toString();
  }
}
