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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryCachingPolicy extends LuceneTestCase {

  public void testLargeSegmentDetection() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(new Document());
    }
    final IndexReader reader = w.getReader();
    for (float minSizeRatio : new float[] {Float.MIN_VALUE, 0.01f, 0.1f, 0.9f}) {
      final QueryCachingPolicy policy = new QueryCachingPolicy.CacheOnLargeSegments(0, minSizeRatio);
      for (LeafReaderContext ctx : reader.leaves()) {
        final Query query = new TermQuery(new Term("field", "value"));
        final boolean shouldCache = policy.shouldCache(query, ctx);
        final float sizeRatio = (float) ctx.reader().maxDoc() / reader.maxDoc();
        assertEquals(sizeRatio >= minSizeRatio, shouldCache);
        assertTrue(new QueryCachingPolicy.CacheOnLargeSegments(numDocs, Float.MIN_VALUE).shouldCache(query, ctx));
        assertFalse(new QueryCachingPolicy.CacheOnLargeSegments(numDocs + 1, Float.MIN_VALUE).shouldCache(query, ctx));
      }
    }
    reader.close();
    w.close();
    dir.close();
  }

}
