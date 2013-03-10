package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTermVectors extends LuceneTestCase {
  private static IndexReader reader;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {                  
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true)).setMergePolicy(newLogMergePolicy()));
    //writer.setUseCompoundFile(true);
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      FieldType ft = new FieldType(TextField.TYPE_STORED);
      int mod3 = i % 3;
      int mod2 = i % 2;
      if (mod2 == 0 && mod3 == 0) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorOffsets(true);
        ft.setStoreTermVectorPositions(true);
      } else if (mod2 == 0) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorPositions(true);
      } else if (mod3 == 0) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorOffsets(true);
      } else {
        ft.setStoreTermVectors(true);
      }
      doc.add(new Field("field", English.intToEnglish(i), ft));
      //test no term vectors too
      doc.add(new TextField("noTV", English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    reader = null;
    directory = null;
  }

  // In a single doc, for the same field, mix the term
  // vectors up
  public void testMixedVectrosVectors() throws IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random(), MockTokenizer.SIMPLE, true)).setOpenMode(OpenMode.CREATE));
    Document doc = new Document();
    
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setStoreTermVectors(true);
    
    FieldType ft3 = new FieldType(TextField.TYPE_STORED);
    ft3.setStoreTermVectors(true);
    ft3.setStoreTermVectorPositions(true);
    
    FieldType ft4 = new FieldType(TextField.TYPE_STORED);
    ft4.setStoreTermVectors(true);
    ft4.setStoreTermVectorOffsets(true);
    
    FieldType ft5 = new FieldType(TextField.TYPE_STORED);
    ft5.setStoreTermVectors(true);
    ft5.setStoreTermVectorOffsets(true);
    ft5.setStoreTermVectorPositions(true);
    
    doc.add(newTextField("field", "one", Field.Store.YES));
    doc.add(newField("field", "one", ft2));
    doc.add(newField("field", "one", ft3));
    doc.add(newField("field", "one", ft4));
    doc.add(newField("field", "one", ft5));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);

    Query query = new TermQuery(new Term("field", "one"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    Fields vectors = searcher.reader.getTermVectors(hits[0].doc);
    assertNotNull(vectors);
    assertEquals(1, vectors.size());
    Terms vector = vectors.terms("field");
    assertNotNull(vector);
    assertEquals(1, vector.size());
    TermsEnum termsEnum = vector.iterator(null);
    assertNotNull(termsEnum.next());
    assertEquals("one", termsEnum.term().utf8ToString());
    assertEquals(5, termsEnum.totalTermFreq());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(5, dpEnum.freq());
    for(int i=0;i<5;i++) {
      assertEquals(i, dpEnum.nextPosition());
    }

    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(5, dpEnum.freq());
    for(int i=0;i<5;i++) {
      dpEnum.nextPosition();
      assertEquals(4*i, dpEnum.startOffset());
      assertEquals(4*i+3, dpEnum.endOffset());
    }
    reader.close();
  }

  private IndexWriter createWriter(Directory dir) throws IOException {
    return new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random())).setMaxBufferedDocs(2));
  }

  private void createDir(Directory dir) throws IOException {
    IndexWriter writer = createWriter(dir);
    writer.addDocument(createDoc());
    writer.close();
  }

  private Document createDoc() {
    Document doc = new Document();
    final FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    doc.add(newField("c", "aaa", ft));
    return doc;
  }

  private void verifyIndex(Directory dir) throws IOException {
    IndexReader r = DirectoryReader.open(dir);
    int numDocs = r.numDocs();
    for (int i = 0; i < numDocs; i++) {
      assertNotNull("term vectors should not have been null for document " + i, r.getTermVectors(i).terms("c"));
    }
    r.close();
  }
  
  public void testFullMergeAddDocs() throws Exception {
    Directory target = newDirectory();
    IndexWriter writer = createWriter(target);
    // with maxBufferedDocs=2, this results in two segments, so that forceMerge
    // actually does something.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(createDoc());
    }
    writer.forceMerge(1);
    writer.close();
    
    verifyIndex(target);
    target.close();
  }

  public void testFullMergeAddIndexesDir() throws Exception {
    Directory[] input = new Directory[] { newDirectory(), newDirectory() };
    Directory target = newDirectory();
    
    for (Directory dir : input) {
      createDir(dir);
    }
    
    IndexWriter writer = createWriter(target);
    writer.addIndexes(input);
    writer.forceMerge(1);
    writer.close();

    verifyIndex(target);

    IOUtils.close(target, input[0], input[1]);
  }
  
  public void testFullMergeAddIndexesReader() throws Exception {
    Directory[] input = new Directory[] { newDirectory(), newDirectory() };
    Directory target = newDirectory();
    
    for (Directory dir : input) {
      createDir(dir);
    }
    
    IndexWriter writer = createWriter(target);
    for (Directory dir : input) {
      IndexReader r = DirectoryReader.open(dir);
      writer.addIndexes(r);
      r.close();
    }
    writer.forceMerge(1);
    writer.close();
    
    verifyIndex(target);
    IOUtils.close(target, input[0], input[1]);
  }

}
