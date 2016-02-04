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
package org.apache.lucene.queries.payloads;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPayloadScoreQuery extends LuceneTestCase {

  private static void checkQuery(SpanQuery query, PayloadFunction function, int[] expectedDocs, float[] expectedScores) throws IOException {
    checkQuery(query, function, true, expectedDocs, expectedScores);
  }

  private static void checkQuery(SpanQuery query, PayloadFunction function, boolean includeSpanScore, int[] expectedDocs, float[] expectedScores) throws IOException {

    assertTrue("Expected docs and scores arrays must be the same length!", expectedDocs.length == expectedScores.length);

    PayloadScoreQuery psq = new PayloadScoreQuery(query, function, includeSpanScore);
    TopDocs hits = searcher.search(psq, expectedDocs.length);

    for (int i = 0; i < hits.scoreDocs.length; i++) {
      if (i > expectedDocs.length - 1)
        fail("Unexpected hit in document " + hits.scoreDocs[i].doc);
      if (hits.scoreDocs[i].doc != expectedDocs[i])
        fail("Unexpected hit in document " + hits.scoreDocs[i].doc);
      assertEquals("Bad score in document " + expectedDocs[i], expectedScores[i], hits.scoreDocs[i].score, 0.000001);
    }

    if (hits.scoreDocs.length > expectedDocs.length)
      fail("Unexpected hit in document " + hits.scoreDocs[expectedDocs.length]);

    QueryUtils.check(random(), psq, searcher);
  }

  @Test
  public void testTermQuery() throws IOException {

    SpanTermQuery q = new SpanTermQuery(new Term("field", "eighteen"));
    for (PayloadFunction fn
        : new PayloadFunction[]{ new AveragePayloadFunction(), new MaxPayloadFunction(), new MinPayloadFunction() }) {
      checkQuery(q, fn, new int[]{ 118, 218, 18 },
                        new float[] { 4.0f, 4.0f, 2.0f });
    }

  }

  @Test
  public void testOrQuery() throws IOException {

    SpanOrQuery q = new SpanOrQuery(new SpanTermQuery(new Term("field", "eighteen")),
                                    new SpanTermQuery(new Term("field", "nineteen")));
    for (PayloadFunction fn
        : new PayloadFunction[]{ new AveragePayloadFunction(), new MaxPayloadFunction(), new MinPayloadFunction() }) {
      checkQuery(q, fn, new int[]{ 118, 119, 218, 219, 18, 19 },
          new float[] { 4.0f, 4.0f, 4.0f, 4.0f, 2.0f, 2.0f });
    }

  }

  @Test
  public void testNearQuery() throws IOException {

    //   2     4
    // twenty two
    //  2     4      4     4
    // one hundred twenty two

    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
                        new SpanTermQuery(new Term("field", "twenty")),
                        new SpanTermQuery(new Term("field", "two"))
                      }, 0, true);

    checkQuery(q, new MaxPayloadFunction(), new int[]{ 22, 122, 222 }, new float[]{ 4.0f, 4.0f, 4.0f });
    checkQuery(q, new MinPayloadFunction(), new int[]{ 122, 222, 22 }, new float[]{ 4.0f, 4.0f, 2.0f });
    checkQuery(q, new AveragePayloadFunction(), new int[] { 122, 222, 22 }, new float[] { 4.0f, 4.0f, 3.0f });

  }

  @Test
  public void testNestedNearQuery() throws Exception {

    // (one OR hundred) NEAR (twenty two) ~ 1
    //  2    4        4    4
    // one hundred twenty two
    // two hundred twenty two

    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanOrQuery(new SpanTermQuery(new Term("field", "one")), new SpanTermQuery(new Term("field", "hundred"))),
        new SpanNearQuery(new SpanQuery[]{
            new SpanTermQuery(new Term("field", "twenty")),
            new SpanTermQuery(new Term("field", "two"))
        }, 0, true)
    }, 1, true);

    // check includeSpanScore makes a difference here
    searcher.setSimilarity(new MultiplyingSimilarity());
    try {
      checkQuery(q, new MaxPayloadFunction(), new int[]{ 122, 222 }, new float[]{ 41.737300872802734f, 34.07836151123047f });
      checkQuery(q, new MinPayloadFunction(), new int[]{ 222, 122 }, new float[]{ 34.07836151123047f, 20.868650436401367f });
      checkQuery(q, new AveragePayloadFunction(), new int[] { 122, 222 }, new float[]{ 38.259193420410156f, 34.07836151123047f });
      checkQuery(q, new MaxPayloadFunction(), false, new int[]{122, 222}, new float[]{4.0f, 4.0f});
      checkQuery(q, new MinPayloadFunction(), false, new int[]{222, 122}, new float[]{4.0f, 2.0f});
      checkQuery(q, new AveragePayloadFunction(), false, new int[]{222, 122}, new float[]{4.0f, 3.666666f});
    }
    finally {
      searcher.setSimilarity(similarity);
    }

  }

  @Test
  public void testSpanContainingQuery() throws Exception {

    // twenty WITHIN ((one OR hundred) NEAR two)~2
    SpanContainingQuery q = new SpanContainingQuery(
        new SpanNearQuery(new SpanQuery[]{
            new SpanOrQuery(new SpanTermQuery(new Term("field", "one")), new SpanTermQuery(new Term("field", "hundred"))),
            new SpanTermQuery(new Term("field", "two"))
        }, 2, true),
        new SpanTermQuery(new Term("field", "twenty"))
    );

    checkQuery(q, new AveragePayloadFunction(), new int[] { 222, 122 }, new float[]{ 4.0f, 3.666666f });
    checkQuery(q, new MaxPayloadFunction(), new int[]{ 122, 222 }, new float[]{ 4.0f, 4.0f });
    checkQuery(q, new MinPayloadFunction(), new int[]{ 222, 122 }, new float[]{ 4.0f, 2.0f });

  }

  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;
  private static BoostingSimilarity similarity = new BoostingSimilarity();
  private static byte[] payload2 = new byte[]{2};
  private static byte[] payload4 = new byte[]{4};

  private static class PayloadAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result));
    }
  }

  private static class PayloadFilter extends TokenFilter {

    private int numSeen = 0;
    private final PayloadAttribute payAtt;

    public PayloadFilter(TokenStream input) {
      super(input);
      payAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      boolean result = false;
      if (input.incrementToken()) {
        if (numSeen % 4 == 0) {
          payAtt.setPayload(new BytesRef(payload2));
        } else {
          payAtt.setPayload(new BytesRef(payload4));
        }
        numSeen++;
        result = true;
      }
      return result;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.numSeen = 0;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new PayloadAnalyzer())
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setSimilarity(similarity));
    //writer.infoStream = System.out;
    for (int i = 0; i < 300; i++) {
      Document doc = new Document();
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      String txt = English.intToEnglish(i) +' '+English.intToEnglish(i+1);
      doc.add(newTextField("field2", txt, Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();

    searcher = newSearcher(reader);
    searcher.setSimilarity(similarity);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }

  static class MultiplyingSimilarity extends DefaultSimilarity {

    @Override
    public float scorePayload(int docId, int start, int end, BytesRef payload) {
      //we know it is size 4 here, so ignore the offset/length
      return payload.bytes[payload.offset];
    }

  }

  static class BoostingSimilarity extends MultiplyingSimilarity {

    @Override
    public float queryNorm(float sumOfSquaredWeights) {
      return 1.0f;
    }

    @Override
    public float coord(int overlap, int maxOverlap) {
      return 1.0f;
    }

    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    //Make everything else 1 so we see the effect of the payload
    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    @Override
    public float lengthNorm(FieldInvertState state) {
      return state.getBoost();
    }

    @Override
    public float sloppyFreq(int distance) {
      return 1.0f;
    }

    @Override
    public float tf(float freq) {
      return 1.0f;
    }

    // idf used for phrase queries
    @Override
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] termStats) {
      return Explanation.match(1.0f, "Inexplicable");
    }

    @Override
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
      return Explanation.match(1.0f, "Inexplicable");
    }

  }

}
