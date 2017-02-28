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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryBitSetProducer extends LuceneTestCase {

  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    w.addDocument(new Document());
    DirectoryReader reader = w.getReader();

    QueryBitSetProducer producer = new QueryBitSetProducer(new MatchNoDocsQuery());
    assertNull(producer.getBitSet(reader.leaves().get(0)));
    assertEquals(1, producer.cache.size());

    producer = new QueryBitSetProducer(new MatchAllDocsQuery());
    BitSet bitSet = producer.getBitSet(reader.leaves().get(0));
    assertEquals(1, bitSet.length());
    assertEquals(true, bitSet.get(0));
    assertEquals(1, producer.cache.size());

    IOUtils.close(reader, w, dir);
  }

  public void testReaderNotSuitedForCaching() throws IOException{
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    w.addDocument(new Document());
    DirectoryReader reader = new DummyDirectoryReader(w.getReader());

    QueryBitSetProducer producer = new QueryBitSetProducer(new MatchNoDocsQuery());
    assertNull(producer.getBitSet(reader.leaves().get(0)));
    assertEquals(0, producer.cache.size());

    producer = new QueryBitSetProducer(new MatchAllDocsQuery());
    BitSet bitSet = producer.getBitSet(reader.leaves().get(0));
    assertEquals(1, bitSet.length());
    assertEquals(true, bitSet.get(0));
    assertEquals(0, producer.cache.size());

    IOUtils.close(reader, w, dir);
  }

  // a reader whose sole purpose is to not be cacheable
  private static class DummyDirectoryReader extends FilterDirectoryReader {

    public DummyDirectoryReader(DirectoryReader in) throws IOException {
      super(in, new SubReaderWrapper() {
        @Override
        public LeafReader wrap(LeafReader reader) {
          return new FilterLeafReader(reader) {

            @Override
            public CacheHelper getCoreCacheHelper() {
              return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
              return null;
            }};
        }
      });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new DummyDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
