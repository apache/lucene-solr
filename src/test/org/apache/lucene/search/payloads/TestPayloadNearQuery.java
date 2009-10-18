package org.apache.lucene.search.payloads;
/**
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
import java.io.Reader;
import java.util.Collection;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;


public class TestPayloadNearQuery extends LuceneTestCase {
  private IndexSearcher searcher;
  private BoostingSimilarity similarity = new BoostingSimilarity();
  private byte[] payload2 = new byte[]{2};
  private byte[] payload4 = new byte[]{4};

  public TestPayloadNearQuery(String s) {
    super(s);
  }

  private class PayloadAnalyzer extends Analyzer {
    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new LowerCaseTokenizer(reader);
      result = new PayloadFilter(result, fieldName);
      return result;
    }
  }

  private class PayloadFilter extends TokenFilter {
    String fieldName;
    int numSeen = 0;
    protected PayloadAttribute payAtt;

    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
      payAtt = (PayloadAttribute) addAttribute(PayloadAttribute.class);
    }

    public boolean incrementToken() throws IOException {
      boolean result = false;
      if (input.incrementToken() == true){
        if (numSeen % 2 == 0) {
          payAtt.setPayload(new Payload(payload2));
        } else {
          payAtt.setPayload(new Payload(payload4));
        }
        numSeen++;
        result = true;
      }
      return result;
    }
  }
  
  private PayloadNearQuery newPhraseQuery (String fieldName, String phrase, boolean inOrder) {
    int n;
    String[] words = phrase.split("[\\s]+");
    SpanQuery clauses[] = new SpanQuery[words.length];
    for (int i=0;i<clauses.length;i++) {
      clauses[i] = new PayloadTermQuery(new Term(fieldName, words[i]), new AveragePayloadFunction());  
    } 
    return new PayloadNearQuery(clauses, 0, inOrder);
  }

  protected void setUp() throws Exception {
    super.setUp();
    RAMDirectory directory = new RAMDirectory();
    PayloadAnalyzer analyzer = new PayloadAnalyzer();
    IndexWriter writer
      = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setSimilarity(similarity);
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new Field("field", English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      String txt = English.intToEnglish(i) +' '+English.intToEnglish(i+1);
      doc.add(new Field("field2",  txt, Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(directory, true);
    searcher.setSimilarity(similarity);
  }

  public void test() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    query = newPhraseQuery("field", "twenty two", true);
    QueryUtils.check(query);
		
    // all 10 hits should have score = 3 because adjacent terms have payloads of 2,4
    // and all the similarity factors are set to 1
    hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("should be 10 hits", hits.totalHits == 10);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);
    }
    for (int i=1;i<10;i++) {
      query = newPhraseQuery("field", English.intToEnglish(i)+" hundred", true);
      // all should have score = 3 because adjacent terms have payloads of 2,4
      // and all the similarity factors are set to 1
      hits = searcher.search(query, null, 100);
      assertTrue("hits is null and it shouldn't be", hits != null);
      assertTrue("should be 100 hits", hits.totalHits == 100);
      for (int j = 0; j < hits.scoreDocs.length; j++) {
        ScoreDoc doc = hits.scoreDocs[j];
        //				System.out.println("Doc: " + doc.toString());
        //				System.out.println("Explain: " + searcher.explain(query, doc.doc));
        assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);
      }
    }
  }


  public void testPayloadNear() throws IOException {
    SpanNearQuery q1, q2;
    PayloadNearQuery query;
    TopDocs hits;
    //SpanNearQuery(clauses, 10000, false)
    q1 = spanNearQuery("field2", "twenty two");
    q2 = spanNearQuery("field2", "twenty three");
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = q1;
    clauses[1] = q2;
    query = new PayloadNearQuery(clauses, 10, false); 
    //System.out.println(query.toString());
    assertEquals(12, searcher.search(query, null, 100).totalHits);
    /*
    System.out.println(hits.totalHits);
    for (int j = 0; j < hits.scoreDocs.length; j++) {
      ScoreDoc doc = hits.scoreDocs[j];
      System.out.println("doc: "+doc.doc+", score: "+doc.score);
    }
    */
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
    query = newPhraseQuery("field", "nine hundred ninety nine", true);
    hits = searcher.search(query, null, 100);
    ScoreDoc doc = hits.scoreDocs[0];
    //		System.out.println("Doc: " + doc.toString());
    //		System.out.println("Explain: " + searcher.explain(query, doc.doc));
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("there should only be one hit", hits.totalHits == 1);
    // should have score = 3 because adjacent terms have payloads of 2,4
    assertTrue(doc.score + " does not equal: " + 3, doc.score == 3); 
  }

  public void testComplexNested() throws IOException {
    PayloadNearQuery query;
    TopDocs hits;

    // combine ordered and unordered spans with some nesting to make sure all payloads are counted

    SpanQuery q1 = newPhraseQuery("field", "nine hundred", true);
    SpanQuery q2 = newPhraseQuery("field", "ninety nine", true);
    SpanQuery q3 = newPhraseQuery("field", "nine ninety", false);
    SpanQuery q4 = newPhraseQuery("field", "hundred nine", false);
    SpanQuery[]clauses = new SpanQuery[] {new PayloadNearQuery(new SpanQuery[] {q1,q2}, 0, true), new PayloadNearQuery(new SpanQuery[] {q3,q4}, 0, false)};
    query = new PayloadNearQuery(clauses, 0, false);
    hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    // should be only 1 hit - doc 999
    assertTrue("should only be one hit", hits.scoreDocs.length == 1);
    // the score should be 3 - the average of all the underlying payloads
    ScoreDoc doc = hits.scoreDocs[0];
    //		System.out.println("Doc: " + doc.toString());
    //		System.out.println("Explain: " + searcher.explain(query, doc.doc));
    assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);  
  }

  // must be static for weight serialization tests 
  static class BoostingSimilarity extends DefaultSimilarity {

    // TODO: Remove warning after API has been finalized
    public float scorePayload(int docId, String fieldName, int start, int end, byte[] payload, int offset, int length) {
      //we know it is size 4 here, so ignore the offset/length
      return payload[0];
    }
    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    //Make everything else 1 so we see the effect of the payload
    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    public float lengthNorm(String fieldName, int numTerms) {
      return 1;
    }

    public float queryNorm(float sumOfSquaredWeights) {
      return 1;
    }

    public float sloppyFreq(int distance) {
      return 1;
    }

    public float coord(int overlap, int maxOverlap) {
      return 1;
    }
    public float tf(float freq) {
      return 1;
    }
    // idf used for phrase queries
    public float idf(Collection terms, Searcher searcher) {
      return 1;
    }
  }
}
