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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.*;
import org.junit.BeforeClass;

public class TestDirectIODirectory extends BaseDirectoryTestCase {

  @BeforeClass
  public static void checkSupported() {
    assumeTrue(
        "This test required a JDK version that has support for ExtendedOpenOption.DIRECT",
        DirectIODirectory.ExtendedOpenOption_DIRECT != null);
  }

  @Override
  protected DirectIODirectory getDirectory(Path path) throws IOException {
    return new DirectIODirectory(
        FSDirectory.open(path), DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE, 0L) {
      @Override
      protected boolean useDirectIO(IOContext context) {
        return true;
      }
    };
  }

  public void testIndexWriteRead() throws IOException {
    try (Directory dir = getDirectory(createTempDir("testDirectIODirectory"))) {
      try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
        Document doc = new Document();
        Field field = newField("field", "foo bar", TextField.TYPE_STORED);
        doc.add(field);

        iw.addDocument(doc);
        iw.commit();
      }

      try (IndexReader ir = DirectoryReader.open(dir)) {
        IndexSearcher s = newSearcher(ir);
        assertEquals(1, s.count(new PhraseQuery("field", "foo", "bar")));
      }
    }
  }

  public void testIllegalEOFWithFileSizeMultipleOfBlockSize() throws Exception {
    // fileSize value smaller than block size may not properly test EOF handling
    // On mac and some linux, block size is 4096; On some windows, block size is 512. Hence picking the larger one here.
    final int fileSize = 4096 * 2;

    try (Directory dir = getDirectory(createTempDir("testIllegalEOF"))) {
      IndexOutput o = dir.createOutput("out", newIOContext(random()));
      byte[] b = new byte[fileSize];
      o.writeBytes(b, 0, fileSize);
      o.close();
      IndexInput i = dir.openInput("out", newIOContext(random()));
      i.seek(fileSize);
      i.close();
    }
  }

}
