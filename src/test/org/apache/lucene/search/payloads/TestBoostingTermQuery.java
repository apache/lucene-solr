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

import junit.framework.TestCase;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;

import java.io.IOException;
import java.io.Reader;

public class TestBoostingTermQuery extends TestCase {
  private IndexSearcher searcher;
  private BoostingSimilarity similarity = new BoostingSimilarity();

  public TestBoostingTermQuery(String s) {
    super(s);
  }

  private class PayloadAnalyzer extends Analyzer {


    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new LowerCaseTokenizer(reader);
      result = new PayloadFilter(result);
      return result;
    }
  }

  private class PayloadFilter extends TokenFilter {


    public PayloadFilter(TokenStream input) {
      super(input);
    }

    public Token next() throws IOException {
      Token result = input.next();
      if (result != null) {
        result.setPayload(new Payload(encodePayload(result.termText()), 0, 4));
      }
      return result;
    }
  }

  protected void setUp() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    PayloadAnalyzer analyzer = new PayloadAnalyzer();
    IndexWriter writer
            = new IndexWriter(directory, analyzer, true);
    writer.setSimilarity(similarity);
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new Field("field", English.intToEnglish(i), Field.Store.YES, Field.Index.TOKENIZED));
      writer.addDocument(doc);
    }
    //writer.optimize();
    writer.close();

    searcher = new IndexSearcher(directory);
    searcher.setSimilarity(similarity);
  }

  private byte[] encodePayload(String englishInt)
  {
    int i = englishInt.hashCode();
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (i >>> 24);
    bytes[1] = (byte) (i >>> 16);
    bytes[2] = (byte) (i >>> 8);
    bytes[3] = (byte) i;
    return bytes;
  }

  private int decodePayload(byte[] payload, int size)
  {
    //This should be equal to the hash code of the String representing the English int from English.intToEnglish
    int result = (payload[0] << 24) | (payload[1] << 16) | (payload[2] << 8) | (payload[3]);
    
    /*assertEquals((byte) (size >>> 24), payload[0]);
    assertEquals((byte) (size >>> 16), payload[1]);
    assertEquals((byte) (size >>> 8), payload[2]);
    assertEquals((byte) size, payload[3]);*/

    return result;
  }

  protected void tearDown() {

  }

  public void test() throws IOException {
    BoostingTermQuery query = new BoostingTermQuery(new Term("field", "seventy"));
    TopDocs hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("hits Size: " + hits.totalHits + " is not: " + 100, hits.totalHits == 100);

    //they should all have the exact same score, because they all contain seventy once, and we set
    //all the other similarity factors to be 1
    //This score should be 1, since we normalize scores
    int seventyHash = "seventy".hashCode();
    assertTrue("score " + hits.getMaxScore() + " does not equal 'seventy' hashcode: " + seventyHash, hits.getMaxScore() == seventyHash);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      ScoreDoc doc = hits.scoreDocs[i];
      assertTrue("score " + doc.score + " does not equal 'seventy' hashcode: " + seventyHash, doc.score == seventyHash);
    }
    CheckHits.checkExplanations(query, "field", searcher);
    Spans spans = query.getSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    assertTrue("spans is not an instanceof " + TermSpans.class, spans instanceof TermSpans);
    /*float score = hits.score(0);
    for (int i =1; i < hits.length(); i++)
    {
      assertTrue("scores are not equal and they should be", score == hits.score(i));
    }*/

  }

  public void testNoMatch() throws Exception {
    BoostingTermQuery query = new BoostingTermQuery(new Term("field", "junk"));
    TopDocs hits = searcher.search(query, null, 100);
    assertTrue("hits is null and it shouldn't be", hits != null);
    assertTrue("hits Size: " + hits.totalHits + " is not: " + 0, hits.totalHits == 0);

  }


  class BoostingSimilarity extends DefaultSimilarity
  {

    // TODO: Remove warning after API has been finalized
    public float scorePayload(byte[] payload, int offset, int length) {
      //we know it is size 4 here, so ignore the offset/length
      return decodePayload(payload,4);
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
      return 1;
    }
  }
}