package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.LuceneTestCase;


public class TestPKIndexSplitter extends LuceneTestCase {
  public void testSplit() throws Exception {
    NumberFormat format = new DecimalFormat("000000000");
    
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(
        Version.LUCENE_CURRENT,
        new WhitespaceAnalyzer(Version.LUCENE_CURRENT))
        .setOpenMode(OpenMode.CREATE));
    for (int x=0; x < 10; x++) {
      Document doc = createDocument(x, "1", 3, format);
      w.addDocument(doc);
    }
    for (int x=15; x < 20; x++) {
      Document doc = createDocument(x, "2", 3, format);
      w.addDocument(doc);
    }
    w.close();
    
    Directory dir1 = newDirectory();
    Directory dir2 = newDirectory();
    Term splitTerm = new Term("id", new BytesRef(format.format(11)));
    PKIndexSplitter splitter = new PKIndexSplitter(splitTerm, 
        dir, dir1, dir2);
    splitter.split();
    
    IndexReader ir1 = IndexReader.open(dir1);
    IndexReader ir2 = IndexReader.open(dir2);
    assertEquals(10, ir1.maxDoc());
    assertEquals(4, ir2.maxDoc());
    
    ir1.close();
    ir2.close();
    
    dir1.close();
    dir2.close();
    dir.close();
  }
  
  public Document createDocument(int n, String indexName, 
      int numFields, NumberFormat format) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    String id = format.format(n);
    doc.add(new Field("id", id, Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("indexname", indexName, Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    sb.append("a");
    sb.append(n);
    doc.add(new Field("field1", sb.toString(), Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i + 1), sb.toString(), Store.YES,
                        Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    }
    return doc;
  }
}
