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

import java.util.Random;
import java.util.concurrent.Executor;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestMultiSliceMerge extends LuceneTestCase {
  Directory dir1;
  Directory dir2;
  IndexReader reader1;
  IndexReader reader2;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir1 = newDirectory();
    dir2 = newDirectory();
    Random random = random();
    RandomIndexWriter iw1 = new RandomIndexWriter(random(), dir1, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
      doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
      doc.add(new SortedDocValuesField("field2", new BytesRef(Boolean.toString(i % 2 == 0))));
      iw1.addDocument(doc);

      if (random.nextBoolean()) {
        iw1.getReader().close();
      }
    }
    reader1 = iw1.getReader();
    iw1.close();

    RandomIndexWriter iw2 = new RandomIndexWriter(random(), dir2, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
      doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
      doc.add(new SortedDocValuesField("field2", new BytesRef(Boolean.toString(i % 2 == 0))));
      iw2.addDocument(doc);

      if (random.nextBoolean()) {
        iw2.commit();
      }
    }
    reader2 = iw2.getReader();
    iw2.close();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    reader1.close();
    reader2.close();
    dir1.close();
    dir2.close();
  }

  public void testMultipleSlicesOfSameIndexSearcher() throws Exception {
    Executor executor1 = runnable -> runnable.run();
    Executor executor2 = runnable -> runnable.run();

    IndexSearcher searchers[] = new IndexSearcher[] {
        new IndexSearcher(reader1, executor1),
        new IndexSearcher(reader2, executor2)
    };

    Query query = new MatchAllDocsQuery();

    TopDocs topDocs1 = searchers[0].search(query, Integer.MAX_VALUE);
    TopDocs topDocs2 = searchers[1].search(query, Integer.MAX_VALUE);

    CheckHits.checkEqual(query, topDocs1.scoreDocs, topDocs2.scoreDocs);
  }

  public void testMultipleSlicesOfMultipleIndexSearchers() throws Exception {
    Executor executor1 = runnable -> runnable.run();
    Executor executor2 = runnable -> runnable.run();

    IndexSearcher searchers[] = new IndexSearcher[] {
        new IndexSearcher(reader1, executor1),
        new IndexSearcher(reader2, executor2)
    };

    Query query = new MatchAllDocsQuery();

    TopDocs topDocs1 = searchers[0].search(query, Integer.MAX_VALUE);
    TopDocs topDocs2 = searchers[1].search(query, Integer.MAX_VALUE);

    assertEquals(topDocs1.scoreDocs.length, topDocs2.scoreDocs.length);

    for (int i = 0; i < topDocs1.scoreDocs.length; i++) {
      topDocs1.scoreDocs[i].shardIndex = 0;
      topDocs2.scoreDocs[i].shardIndex = 1;
    }

    TopDocs[] shardHits = {topDocs1, topDocs2};

    TopDocs mergedHits1 = TopDocs.merge(0, topDocs1.scoreDocs.length, shardHits);
    TopDocs mergedHits2 = TopDocs.merge(0, topDocs1.scoreDocs.length, shardHits);

    CheckHits.checkEqual(query, mergedHits1.scoreDocs, mergedHits2.scoreDocs);
  }
}
