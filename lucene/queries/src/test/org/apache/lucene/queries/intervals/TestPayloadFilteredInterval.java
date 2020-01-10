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

package org.apache.lucene.queries.intervals;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.SimplePayloadFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestPayloadFilteredInterval extends LuceneTestCase {

  public void testPayloadFilteredInterval() throws Exception {

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tok = new MockTokenizer(MockTokenizer.SIMPLE, true);
        return new TokenStreamComponents(tok, new SimplePayloadFilter(tok));
      }
    };

    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(analyzer)
            .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000)).setMergePolicy(newLogMergePolicy()));

    Document doc = new Document();
    doc.add(newTextField("field", "a sentence with words repeated words words quite often words", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    // SimplePayloadFilter stores a payload for each term at position n containing
    // the bytes 'pos:n'

    IntervalsSource source = Intervals.term("words", b -> b.utf8ToString().endsWith("5") == false);
    assertEquals("PAYLOAD_FILTERED(words)", source.toString());

    IntervalIterator it = source.intervals("field", reader.leaves().get(0));

    assertEquals(0, it.nextDoc());
    assertEquals(3, it.nextInterval());
    assertEquals(6, it.nextInterval());
    assertEquals(9, it.nextInterval());
    assertEquals(IntervalIterator.NO_MORE_INTERVALS, it.nextInterval());

    MatchesIterator mi = source.matches("field", reader.leaves().get(0), 0);
    assertNotNull(mi);
    assertTrue(mi.next());
    assertEquals(3, mi.startPosition());
    assertTrue(mi.next());
    assertEquals(6, mi.startPosition());
    assertTrue(mi.next());
    assertEquals(9, mi.startPosition());
    assertFalse(mi.next());

    reader.close();
    directory.close();

  }

}
