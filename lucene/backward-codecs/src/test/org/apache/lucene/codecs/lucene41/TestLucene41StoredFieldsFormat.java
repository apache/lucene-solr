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
package org.apache.lucene.codecs.lucene41;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

public class TestLucene41StoredFieldsFormat extends BaseStoredFieldsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new Lucene41RWCodec();
  }

  public void testMergeLargeDocuments() throws IOException {
    // this format has a different logic for blocks that are greater than 32KB (2 * chunkSize)
    // so we need to explicitely test it.
    final int numDocs = atLeast(200);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE));
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("delete", random().nextBoolean() ? "yes" : "no", Store.YES));
      final int length;
      switch (random().nextInt(4)) {
        case 0:
          length = 16; // small docs so that some large block contain several docs (a small doc followed by a large one)
          break;
        case 1:
          length = 1 << 14; // a document that will cause the block NOT to be sliced to make sure that we make the distinction correctly
          break;
        case 2:
          length = 1 << 15; // a document that will make the block sliced into 2 slices
          break;
        case 3:
          length = 1 << 17; // a document that will make the block sliced into more than 2 slices
          break;
        default:
          throw new AssertionError();
      }
      doc.add(new StoredField("f", RandomStrings.randomAsciiOfLength(random(), length)));
      w.addDocument(doc);
    }
    w.deleteDocuments(new Term("delete", "yes"));
    w.commit();
    w.close();

    w = new RandomIndexWriter(random(), dir);
    w.forceMerge(TestUtil.nextInt(random(), 1, 3));
    w.commit();
    w.close();
    TestUtil.checkIndex(dir);
    dir.close();
  }
}
