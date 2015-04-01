package org.apache.lucene.search.spans;

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
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Spans (v2)
 *
 */
public class TestSpansEnum extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;

  static final class SimplePayloadFilter extends TokenFilter {
    int pos;
    final PayloadAttribute payloadAttr;
    final CharTermAttribute termAttr;

    public SimplePayloadFilter(TokenStream input) {
      super(input);
      pos = 0;
      payloadAttr = input.addAttribute(PayloadAttribute.class);
      termAttr = input.addAttribute(CharTermAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        payloadAttr.setPayload(new BytesRef(("pos: " + pos).getBytes(StandardCharsets.UTF_8)));
        pos++;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      pos = 0;
    }
  }

  static Analyzer simplePayloadAnalyzer;
  @BeforeClass
  public static void beforeClass() throws Exception {
    simplePayloadAnalyzer = new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
          return new TokenStreamComponents(tokenizer, new SimplePayloadFilter(tokenizer));
        }
    };

    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(simplePayloadAnalyzer)
            .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000)).setMergePolicy(newLogMergePolicy()));
    //writer.infoStream = System.out;
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    for (int i = 100; i < 110; i++) {
      Document doc = new Document(); // doc id 10-19 have 100-109
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcher = null;
    reader = null;
    directory = null;
    simplePayloadAnalyzer = null;
  }

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, "field", searcher, results);
  }

  SpanTermQuery spanTQ(String term) {
    return new SpanTermQuery(new Term("field", term));
  }

  @Test
  public void testSpansEnumOr1() throws Exception {
    SpanTermQuery t1 = spanTQ("one");
    SpanTermQuery t2 = spanTQ("two");
    SpanOrQuery soq = new SpanOrQuery(t1, t2);
    checkHits(soq, new int[] {1, 2, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  }

  @Test
  public void testSpansEnumOr2() throws Exception {
    SpanTermQuery t1 = spanTQ("one");
    SpanTermQuery t11 = spanTQ("eleven");
    SpanOrQuery soq = new SpanOrQuery(t1, t11);
    checkHits(soq, new int[] {1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  }

  @Test
  public void testSpansEnumOr3() throws Exception {
    SpanTermQuery t12 = spanTQ("twelve");
    SpanTermQuery t11 = spanTQ("eleven");
    SpanOrQuery soq = new SpanOrQuery(t12, t11);
    checkHits(soq, new int[] {});
  }

  @Test
  public void testSpansEnumOrNot1() throws Exception {
    SpanTermQuery t1 = spanTQ("one");
    SpanTermQuery t2 = spanTQ("two");
    SpanOrQuery soq = new SpanOrQuery(t1, t2);
    SpanNotQuery snq = new SpanNotQuery(soq, t1);
    checkHits(snq, new int[] {2,12});
  }

  @Test
  public void testSpansEnumNotBeforeAfter1() throws Exception {
    SpanTermQuery t1 = spanTQ("one");
    SpanTermQuery t100 = spanTQ("hundred");
    SpanNotQuery snq = new SpanNotQuery(t100, t1, 0, 0);
    checkHits(snq, new int[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19}); // include all "one hundred ..."
  }

  @Test
  public void testSpansEnumNotBeforeAfter2() throws Exception {
    SpanTermQuery t1 = spanTQ("one");
    SpanTermQuery t100 = spanTQ("hundred");
    SpanNotQuery snq = new SpanNotQuery(t100, t1, 1, 0);
    checkHits(snq, new int[] {}); // exclude all "one hundred ..."
  }

  @Test
  public void testSpansEnumNotBeforeAfter3() throws Exception {
    SpanTermQuery t1 = spanTQ("one");
    SpanTermQuery t100 = spanTQ("hundred");
    SpanNotQuery snq = new SpanNotQuery(t100, t1, 0, 1);
    checkHits(snq, new int[] {10, 12, 13, 14, 15, 16, 17, 18, 19}); // exclude "one hundred one"
  }
}
