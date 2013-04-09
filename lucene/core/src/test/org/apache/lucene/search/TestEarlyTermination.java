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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestEarlyTermination extends LuceneTestCase {

  Directory dir;
  RandomIndexWriter writer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    writer = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      writer.addDocument(new Document());
      if (rarely()) {
        writer.commit();
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    writer.close();
    dir.close();
  }

  public void testEarlyTermination() throws IOException {
    final int iters = atLeast(5);
    final IndexReader reader = writer.getReader();

    for (int i = 0; i < iters; ++i) {
      final IndexSearcher searcher = newSearcher(reader);
      final Collector collector = new Collector() {

        final boolean outOfOrder = random().nextBoolean();
        boolean collectionTerminated = true;

        @Override
        public void setScorer(Scorer scorer) throws IOException {}

        @Override
        public void collect(int doc) throws IOException {
          assertFalse(collectionTerminated);
          if (rarely()) {
            collectionTerminated = true;
            throw new CollectionTerminatedException();
          }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
          if (random().nextBoolean()) {
            collectionTerminated = true;
            throw new CollectionTerminatedException();
          } else {
            collectionTerminated = false;
          }
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return outOfOrder;
        }

      };

      searcher.search(new MatchAllDocsQuery(), collector);
    }
    reader.close();
  }

}
