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
package org.apache.lucene.search.grouping;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Base class for grouping related tests.
 */
// TODO (MvG) : The grouping tests contain a lot of code duplication. Try to move the common code to this class..
public abstract class AbstractGroupingTestCase extends LuceneTestCase {

  protected String generateRandomNonEmptyString() {
    String randomValue;
    do {
      // B/c of DV based impl we can't see the difference between an empty string and a null value.
      // For that reason we don't generate empty string
      // groups.
      randomValue = TestUtil.randomRealisticUnicodeString(random());
      //randomValue = _TestUtil.randomSimpleString(random());
    } while ("".equals(randomValue));
    return randomValue;
  }

  protected static void assertScoreDocsEquals(ScoreDoc[] expected, ScoreDoc[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].doc, actual[i].doc);
      assertEquals(expected[i].score, actual[i].score, 0);
    }
  }

  protected static class Shard implements Closeable {

    final Directory directory;
    final RandomIndexWriter writer;
    IndexSearcher searcher;

    Shard() throws IOException {
      this.directory = newDirectory();
      this.writer = new RandomIndexWriter(random(), directory,
          newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    }

    IndexSearcher getIndexSearcher() throws IOException {
      if (searcher == null) {
        searcher = new IndexSearcher(this.writer.getReader());
      }
      return searcher;
    }

    @Override
    public void close() throws IOException {
      if (searcher != null) {
        searcher.getIndexReader().close();
      }
      IOUtils.close(writer, directory);
    }
  }
}
