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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FilterDirectoryReader.SubReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestFilterDirectoryReader extends LuceneTestCase {

  private static class DummySubReaderWrapper extends SubReaderWrapper {

    @Override
    public LeafReader wrap(LeafReader reader) {
      return reader;
    }
    
  }

  private static class DummyFilterDirectoryReader extends FilterDirectoryReader {

    public DummyFilterDirectoryReader(DirectoryReader in) throws IOException {
      super(in, new DummySubReaderWrapper());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new DummyFilterDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
    
  }

  public void testDoubleClose() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());

    DirectoryReader reader = DirectoryReader.open(w);
    DirectoryReader wrapped = new DummyFilterDirectoryReader(reader);

    // Calling close() on the original reader and wrapped reader should only close
    // the original reader once (as per Closeable.close() contract that close() is
    // idempotent)
    List<DirectoryReader> readers = Arrays.asList(reader, wrapped);
    Collections.shuffle(readers, random());
    IOUtils.close(readers);

    w.close();
    dir.close();
  }

  private static class NumDocsCountingSubReaderWrapper extends SubReaderWrapper {

    private final AtomicLong numDocsCallCount;

    NumDocsCountingSubReaderWrapper(AtomicLong numDocsCallCount) {
      this.numDocsCallCount = numDocsCallCount;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      return new FilterLeafReader(reader) {
        @Override
        public int numDocs() {
          numDocsCallCount.incrementAndGet();
          return super.numDocs();
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
          return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return in.getReaderCacheHelper();
        }
      };
    }

  }

  private static class NumDocsCountingFilterDirectoryReader extends FilterDirectoryReader {

    private final AtomicLong numDocsCallCount;

    public NumDocsCountingFilterDirectoryReader(DirectoryReader in, AtomicLong numDocsCallCount) throws IOException {
      super(in, new NumDocsCountingSubReaderWrapper(numDocsCallCount));
      this.numDocsCallCount = numDocsCallCount;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new NumDocsCountingFilterDirectoryReader(in, numDocsCallCount);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }

  }

  public void testFilterDirectoryReaderNumDocsIsLazy() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    DirectoryReader directoryReader = DirectoryReader.open(w);
    w.close();

    AtomicLong numDocsCallCount = new AtomicLong();
    DirectoryReader directoryReaderWrapper = new NumDocsCountingFilterDirectoryReader(directoryReader, numDocsCallCount);
    assertEquals(0L, numDocsCallCount.get());
    assertEquals(1, directoryReaderWrapper.numDocs());
    assertEquals(1L, numDocsCallCount.get()); // one segment, so called once
    assertEquals(1, directoryReaderWrapper.numDocs());
    assertEquals(1L, numDocsCallCount.get());

    directoryReader.close();
    dir.close();
  }

}
