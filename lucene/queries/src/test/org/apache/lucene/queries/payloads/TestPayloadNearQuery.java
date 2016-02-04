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
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestPayloadNearQuery extends LuceneTestCase {
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
      return new TokenStreamComponents(result, new PayloadFilter(result, fieldName));
    }
  }

  private static class PayloadFilter extends TokenFilter {
    private final String fieldName;
    private int numSeen = 0;
    private final PayloadAttribute payAtt;

    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
      payAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      boolean result = false;
      if (input.incrementToken()) {
        if (numSeen % 2 == 0) {
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
  
  private PayloadNearQuery newPhraseQuery (String fieldName, String phrase, boolean inOrder, PayloadFunction function ) {
    String[] words = phrase.split("[\\s]+");
    SpanQuery clauses[] = new SpanQuery[words.length];
    for (int i=0;i<clauses.length;i++) {
      clauses[i] = new SpanTermQuery(new Term(fieldName, words[i]));  
    } 
    return new PayloadNearQuery(clauses, 0, inOrder, function);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(new PayloadAnalyzer())
        .setSimilarity(similarity));
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
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

  public void test() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    query = newPhraseQuery("field", "twenty two", true, new AveragePayloadFunction());
    QueryUtils.check(query);

    // all 10 hits should have score = 3 because adjacent terms have payloads of 2,4
    // and all the similarity factors are set to 1
    hits = searcher.search(query, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("should be 10 hits", hits.totalHits == 10);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);
    }
    for (int i=1;i<10;i++) {
      query = newPhraseQuery("field", English.intToEnglish(i)+" hundred", true, new AveragePayloadFunction());
      if (VERBOSE) {
        System.out.println("TEST: run query=" + query);
      }
      // all should have score = 3 because adjacent terms have payloads of 2,4
      // and all the similarity factors are set to 1
      hits = searcher.search(query, 100);
      assertTrue("hits is null and it shouldn't be", hits != null);
      assertEquals("should be 100 hits", 100, hits.totalHits);
      for (int j = 0; j < hits.scoreDocs.length; j++) {
        ScoreDoc doc = hits.scoreDocs[j];
        //        System.out.println("Doc: " + doc.toString());
        //        System.out.println("Explain: " + searcher.explain(query, doc.doc));
        assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);
      }
    }
  }


  public void testPayloadNear() throws IOException {
    SpanNearQuery q1, q2;
    PayloadNearQuery query;
    //SpanNearQuery(clauses, 10000, false)
    q1 = spanNearQuery("field2", "twenty two");
    q2 = spanNearQuery("field2", "twenty three");
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = q1;
    clauses[1] = q2;
    query = new PayloadNearQuery(clauses, 10, false); 
    //System.out.println(query.toString());
    assertEquals(12, searcher.search(query, 100).totalHits);
    /*
    System.out.println(hits.totalHits);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      System.out.println("doc: "+doc.doc+", score: "+doc.score);
    }
    */
  }
  
  public void testAverageFunction() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    query = newPhraseQuery("field", "twenty two", true, new AveragePayloadFunction());
    QueryUtils.check(query);
    // all 10 hits should have score = 3 because adjacent terms have payloads of 2,4
    // and all the similarity factors are set to 1
    hits = searcher.search(query, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("should be 10 hits", hits.totalHits == 10);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);
      Explanation explain = searcher.explain(query, hits.scoreDocs[j].doc);
      String exp = explain.toString();
      assertTrue(exp, exp.indexOf("AveragePayloadFunction") > -1);
      assertTrue(hits.scoreDocs[j].score + " explain value does not equal: " + 3, explain.getValue() == 3f);
    }
  }
  public void testMaxFunction() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    query = newPhraseQuery("field", "twenty two", true, new MaxPayloadFunction());
    QueryUtils.check(query);
    // all 10 hits should have score = 4 (max payload value)
    hits = searcher.search(query, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("should be 10 hits", hits.totalHits == 10);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      assertTrue(doc.score + " does not equal: " + 4, doc.score == 4);
      Explanation explain = searcher.explain(query, hits.scoreDocs[j].doc);
      String exp = explain.toString();
      assertTrue(exp, exp.indexOf("MaxPayloadFunction") > -1);
      assertTrue(hits.scoreDocs[j].score + " explain value does not equal: " + 4, explain.getValue() == 4f);
    }
  }  
  public void testMinFunction() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    query = newPhraseQuery("field", "twenty two", true, new MinPayloadFunction());
    QueryUtils.check(query);
    // all 10 hits should have score = 2 (min payload value)
    hits = searcher.search(query, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("should be 10 hits", hits.totalHits == 10);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      assertTrue(doc.score + " does not equal: " + 2, doc.score == 2);
      Explanation explain = searcher.explain(query, hits.scoreDocs[j].doc);
      String exp = explain.toString();
      assertTrue(exp, exp.indexOf("MinPayloadFunction") > -1);
      assertTrue(hits.scoreDocs[j].score + " explain value does not equal: " + 2, explain.getValue() == 2f);
    }
  }  
  private SpanQuery[] getClauses() {
      SpanNearQuery q1, q2;
      q1 = spanNearQuery("field2", "twenty two");
      q2 = spanNearQuery("field2", "twenty three");
      SpanQuery[] clauses = new SpanQuery[2];
      clauses[0] = q1;
      clauses[1] = q2;
      return clauses;
  }
  private SpanNearQuery spanNearQuery(String fieldName, String words) {
    String[] wordList = words.split("[\\s]+");
    SpanQuery clauses[] = new SpanQuery[wordList.length];
    for (int i=0;i<clauses.length;i++) {
      clauses[i] = new PayloadTermQuery(new Term(fieldName, wordList[i]), new AveragePayloadFunction());
    } 
    return new SpanNearQuery(clauses, 10000, false);
  }

  public void testLongerSpan() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;
    query = newPhraseQuery("field", "nine hundred ninety nine", true, new AveragePayloadFunction());
    hits = searcher.search(query, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    ScoreDoc doc = hits.scoreDocs[0];
    //    System.out.println("Doc: " + doc.toString());
    //    System.out.println("Explain: " + searcher.explain(query, doc.doc));
    assertTrue("there should only be one hit", hits.totalHits == 1);
    // should have score = 3 because adjacent terms have payloads of 2,4
    assertTrue(doc.score + " does not equal: " + 3, doc.score == 3); 
  }

  public void testComplexNested() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    // combine ordered and unordered spans with some nesting to make sure all payloads are counted

    SpanQuery q1 = newPhraseQuery("field", "nine hundred", true, new AveragePayloadFunction());
    SpanQuery q2 = newPhraseQuery("field", "ninety nine", true, new AveragePayloadFunction());
    SpanQuery q3 = newPhraseQuery("field", "nine ninety", false, new AveragePayloadFunction());
    SpanQuery q4 = newPhraseQuery("field", "hundred nine", false, new AveragePayloadFunction());
    SpanQuery[]clauses = new SpanQuery[] {new PayloadNearQuery(new SpanQuery[] {q1,q2}, 0, true), new PayloadNearQuery(new SpanQuery[] {q3,q4}, 0, false)};
    query = new PayloadNearQuery(clauses, 0, false);
    hits = searcher.search(query, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    // should be only 1 hit - doc 999
    assertTrue("should only be one hit", hits.scoreDocs.length == 1);
    // the score should be 3 - the average of all the underlying payloads
    ScoreDoc doc = hits.scoreDocs[0];
    //    System.out.println("Doc: " + doc.toString());
    //    System.out.println("Explain: " + searcher.explain(query, doc.doc));
    assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);  
  }

  static class BoostingSimilarity extends DefaultSimilarity {

    @Override
    public float queryNorm(float sumOfSquaredWeights) {
      return 1.0f;
    }
    
    @Override
    public float coord(int overlap, int maxOverlap) {
      return 1.0f;
    }
    
    @Override 
    public float scorePayload(int docId, int start, int end, BytesRef payload) {
      //we know it is size 4 here, so ignore the offset/length
      return payload.bytes[payload.offset];
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
  }
}
