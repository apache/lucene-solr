package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorVisitor;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestContribParallelReader extends LuceneTestCase {

  private IndexSearcher parallel;
  private IndexSearcher single;
  private Directory dir, dir1, dir2;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    single = single(random);
    parallel = parallel(random);
  }
  
  @Override
  public void tearDown() throws Exception {
    single.getIndexReader().close();
    single.close();
    parallel.getIndexReader().close();
    parallel.close();
    dir.close();
    dir1.close();
    dir2.close();
    super.tearDown();
  }

  // Fields 1-4 indexed together:
  private IndexSearcher single(Random random) throws IOException {
    dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d1 = new Document();
    d1.add(newField("f1", "v1", TextField.TYPE_STORED));
    d1.add(newField("f2", "v1", TextField.TYPE_STORED));
    d1.add(newField("f3", "v1", TextField.TYPE_STORED));
    d1.add(newField("f4", "v1", TextField.TYPE_STORED));
    w.addDocument(d1);
    Document d2 = new Document();
    d2.add(newField("f1", "v2", TextField.TYPE_STORED));
    d2.add(newField("f2", "v2", TextField.TYPE_STORED));
    d2.add(newField("f3", "v2", TextField.TYPE_STORED));
    d2.add(newField("f4", "v2", TextField.TYPE_STORED));
    w.addDocument(d2);
    w.close();

    return new IndexSearcher(dir, false);
  }

  // Fields 1 & 2 in one index, 3 & 4 in other, with ParallelReader:
  private IndexSearcher parallel(Random random) throws IOException {
    dir1 = getDir1(random);
    dir2 = getDir2(random);
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1, false));
    pr.add(IndexReader.open(dir2, false));
    return newSearcher(pr);
  }

  private Document getDocument(IndexReader ir, int docID, FieldSelector selector) throws IOException {
    final FieldSelectorVisitor visitor = new FieldSelectorVisitor(selector);
    ir.document(docID, visitor);
    return visitor.getDocument();
  }

  public void testDocument() throws IOException {
    Directory dir1 = getDir1(random);
    Directory dir2 = getDir2(random);
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1, false));
    pr.add(IndexReader.open(dir2, false));

    Document doc11 = getDocument(pr, 0, new MapFieldSelector("f1"));
    Document doc24 = getDocument(pr, 1, new MapFieldSelector(Arrays.asList("f4")));
    Document doc223 = getDocument(pr, 1, new MapFieldSelector("f2", "f3"));
    
    assertEquals(1, doc11.getFields().size());
    assertEquals(1, doc24.getFields().size());
    assertEquals(2, doc223.getFields().size());
    
    assertEquals("v1", doc11.get("f1"));
    assertEquals("v2", doc24.get("f4"));
    assertEquals("v2", doc223.get("f2"));
    assertEquals("v2", doc223.get("f3"));
    pr.close();
    dir1.close();
    dir2.close();
  }

  private Directory getDir1(Random random) throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d1 = new Document();
    d1.add(newField("f1", "v1", TextField.TYPE_STORED));
    d1.add(newField("f2", "v1", TextField.TYPE_STORED));
    w1.addDocument(d1);
    Document d2 = new Document();
    d2.add(newField("f1", "v2", TextField.TYPE_STORED));
    d2.add(newField("f2", "v2", TextField.TYPE_STORED));
    w1.addDocument(d2);
    w1.close();
    return dir1;
  }

  private Directory getDir2(Random random) throws IOException {
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d3 = new Document();
    d3.add(newField("f3", "v1", TextField.TYPE_STORED));
    d3.add(newField("f4", "v1", TextField.TYPE_STORED));
    w2.addDocument(d3);
    Document d4 = new Document();
    d4.add(newField("f3", "v2", TextField.TYPE_STORED));
    d4.add(newField("f4", "v2", TextField.TYPE_STORED));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }
}
