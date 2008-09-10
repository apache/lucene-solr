package org.apache.lucene.search.payloads;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

public class TestBoostingTermQuery extends LuceneTestCase {
  private IndexSearcher searcher;
  private BoostingSimilarity similarity = new BoostingSimilarity();
  private byte[] payloadField = new byte[]{1};
  private byte[] payloadMultiField1 = new byte[]{2};
  private byte[] payloadMultiField2 = new byte[]{4};

  public TestBoostingTermQuery(String s) {
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

    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
    }

    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
      Token nextToken = input.next(reusableToken);
      if (nextToken != null) {
        if (fieldName.equals("field")) {
          nextToken.setPayload(new Payload(payloadField));
        } else if (fieldName.equals("multiField")) {
          if (numSeen % 2 == 0) {
            nextToken.setPayload(new Payload(payloadMultiField1));
          } else {
            nextToken.setPayload(new Payload(payloadMultiField2));
          }
          numSeen++;
        }

      }
      return nextToken;
    }
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
      Field noPayloadField = new Field(PayloadHelper.NO_PAYLOAD_FIELD, English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED);
      //noPayloadField.setBoost(0);
      doc.add(noPayloadField);
      doc.add(new Field("field", English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("multiField", English.intToEnglish(i) + "  " + English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(directory);
    searcher.setSimilarity(similarity);
  }

  public void test() throws IOException {
    BoostingTermQuery query = new BoostingTermQuery(new Term("field", "seventy"));
    TopDocs hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("hits Size: " + hits.totalHits + " is not: " + 100, hits.totalHits == 100);

    //they should all have the exact same score, because they all contain seventy once, and we set
    //all the other similarity factors to be 1

    assertTrue(hits.getMaxScore() + " does not equal: " + 1, hits.getMaxScore() == 1);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      ScoreDoc doc = hits.scoreDocs[i];
      assertTrue(doc.score + " does not equal: " + 1, doc.score == 1);
    }
    CheckHits.checkExplanations(query, PayloadHelper.FIELD, searcher, true);
    Spans spans = query.getSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    assertTrue("spans is not an instanceof " + TermSpans.class, spans instanceof TermSpans);
    /*float score = hits.score(0);
    for (int i =1; i < hits.length(); i++)
    {
      assertTrue("scores are not equal and they should be", score == hits.score(i));
    }*/

  }

  public void testMultipleMatchesPerDoc() throws Exception {
    BoostingTermQuery query = new BoostingTermQuery(new Term(PayloadHelper.MULTI_FIELD, "seventy"));
    TopDocs hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("hits Size: " + hits.totalHits + " is not: " + 100, hits.totalHits == 100);

    //they should all have the exact same score, because they all contain seventy once, and we set
    //all the other similarity factors to be 1

    //System.out.println("Hash: " + seventyHash + " Twice Hash: " + 2*seventyHash);
    assertTrue(hits.getMaxScore() + " does not equal: " + 3, hits.getMaxScore() == 3);
    //there should be exactly 10 items that score a 3, all the rest should score a 2
    //The 10 items are: 70 + i*100 where i in [0-9]
    int numTens = 0;
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      ScoreDoc doc = hits.scoreDocs[i];
      if (doc.doc % 10 == 0) {
        numTens++;
        assertTrue(doc.score + " does not equal: " + 3, doc.score == 3);
      } else {
        assertTrue(doc.score + " does not equal: " + 2, doc.score == 2);
      }
    }
    assertTrue(numTens + " does not equal: " + 10, numTens == 10);
    CheckHits.checkExplanations(query, "field", searcher, true);
    Spans spans = query.getSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    assertTrue("spans is not an instanceof " + TermSpans.class, spans instanceof TermSpans);
    //should be two matches per document
    int count = 0;
    //100 hits times 2 matches per hit, we should have 200 in count
    while (spans.next()) {
      count++;
    }
    assertTrue(count + " does not equal: " + 200, count == 200);
  }

  public void testNoMatch() throws Exception {
    BoostingTermQuery query = new BoostingTermQuery(new Term(PayloadHelper.FIELD, "junk"));
    TopDocs hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("hits Size: " + hits.totalHits + " is not: " + 0, hits.totalHits == 0);

  }

  public void testNoPayload() throws Exception {
    BoostingTermQuery q1 = new BoostingTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "zero"));
    BoostingTermQuery q2 = new BoostingTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "foo"));
    BooleanClause c1 = new BooleanClause(q1, BooleanClause.Occur.MUST);
    BooleanClause c2 = new BooleanClause(q2, BooleanClause.Occur.MUST_NOT);
    BooleanQuery query = new BooleanQuery();
    query.add(c1);
    query.add(c2);
    TopDocs hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("hits Size: " + hits.totalHits + " is not: " + 1, hits.totalHits == 1);
    int[] results = new int[1];
    results[0] = 0;//hits.scoreDocs[0].doc;
    CheckHits.checkHitCollector(query, PayloadHelper.NO_PAYLOAD_FIELD, searcher, results);
  }

  // must be static for weight serialization tests 
  static class BoostingSimilarity extends DefaultSimilarity {

    // TODO: Remove warning after API has been finalized
    public float scorePayload(String fieldName, byte[] payload, int offset, int length) {
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

    public float idf(int docFreq, int numDocs) {
      return 1;
    }

    public float tf(float freq) {
      return freq == 0 ? 0 : 1;
    }
  }
}
