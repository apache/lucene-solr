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

}
