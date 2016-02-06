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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** simple tests for unionpostingsenum */
public class TestMultiPhraseEnum extends LuceneTestCase {
  
  /** Tests union on one document  */
  public void testOneDocument() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter writer = new IndexWriter(dir, iwc);
    
    Document doc = new Document();
    doc.add(new TextField("field", "foo bar", Field.Store.NO));
    writer.addDocument(doc);
    
    DirectoryReader ir = DirectoryReader.open(writer);
    writer.close();

    PostingsEnum p1 = getOnlySegmentReader(ir).postings(new Term("field", "foo"), PostingsEnum.POSITIONS);
    PostingsEnum p2 = getOnlySegmentReader(ir).postings(new Term("field", "bar"), PostingsEnum.POSITIONS);
    PostingsEnum union = new MultiPhraseQuery.UnionPostingsEnum(Arrays.asList(p1, p2));
    
    assertEquals(-1, union.docID());
    
    assertEquals(0, union.nextDoc());
    assertEquals(2, union.freq());
    assertEquals(0, union.nextPosition());
    assertEquals(1, union.nextPosition());
    
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, union.nextDoc());
    
    ir.close();
    dir.close();
  }
  
  /** Tests union on a few documents  */
  public void testSomeDocuments() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter writer = new IndexWriter(dir, iwc);
    
    Document doc = new Document();
    doc.add(new TextField("field", "foo", Field.Store.NO));
    writer.addDocument(doc);
    
    writer.addDocument(new Document());
    
    doc = new Document();
    doc.add(new TextField("field", "foo bar", Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("field", "bar", Field.Store.NO));
    writer.addDocument(doc);
    
    writer.forceMerge(1);
    DirectoryReader ir = DirectoryReader.open(writer);
    writer.close();

    PostingsEnum p1 = getOnlySegmentReader(ir).postings(new Term("field", "foo"), PostingsEnum.POSITIONS);
    PostingsEnum p2 = getOnlySegmentReader(ir).postings(new Term("field", "bar"), PostingsEnum.POSITIONS);
    PostingsEnum union = new MultiPhraseQuery.UnionPostingsEnum(Arrays.asList(p1, p2));
    
    assertEquals(-1, union.docID());
    
    assertEquals(0, union.nextDoc());
    assertEquals(1, union.freq());
    assertEquals(0, union.nextPosition());
    
    assertEquals(2, union.nextDoc());
    assertEquals(2, union.freq());
    assertEquals(0, union.nextPosition());
    assertEquals(1, union.nextPosition());
    
    assertEquals(3, union.nextDoc());
    assertEquals(1, union.freq());
    assertEquals(0, union.nextPosition());
    
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, union.nextDoc());
    
    ir.close();
    dir.close();
  }
}
