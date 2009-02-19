package org.apache.lucene.search.spans;

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
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.payloads.PayloadHelper;
import org.apache.lucene.search.payloads.PayloadSpanUtil;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;

public class TestPayloadSpans extends TestCase {
  private final static boolean DEBUG = false;
  private IndexSearcher searcher;
  private Similarity similarity = new DefaultSimilarity();
  protected IndexReader indexReader;

  public TestPayloadSpans(String s) {
    super(s);
  }

  protected void setUp() throws IOException {
    PayloadHelper helper = new PayloadHelper();
    searcher = helper.setUp(similarity, 1000);
    indexReader = searcher.getIndexReader();
  }

  protected void tearDown() {

  }

  public void testSpanTermQuery() throws Exception {
    SpanTermQuery stq;
    PayloadSpans spans;
    stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "seventy"));
    spans = stq.getPayloadSpans(indexReader);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 100, 1, 1, 1);

    stq = new SpanTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "seventy"));  
    spans = stq.getPayloadSpans(indexReader);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 100, 0, 0, 0);
  }

  public void testSpanFirst() throws IOException {

    SpanQuery match;
    SpanFirstQuery sfq;
    match = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    sfq = new SpanFirstQuery(match, 2);
    PayloadSpans spans = sfq.getPayloadSpans(indexReader);
    checkSpans(spans, 109, 1, 1, 1);
    //Test more complicated subclause
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "hundred"));
    match = new SpanNearQuery(clauses, 0, true);
    sfq = new SpanFirstQuery(match, 2);
    checkSpans(sfq.getPayloadSpans(indexReader), 100, 2, 1, 1);

    match = new SpanNearQuery(clauses, 0, false);
    sfq = new SpanFirstQuery(match, 2);
    checkSpans(sfq.getPayloadSpans(indexReader), 100, 2, 1, 1);
    
  }
  
  public void testNestedSpans() throws Exception {
    SpanTermQuery stq;
    PayloadSpans spans;
    IndexSearcher searcher = getSearcher();
    stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "mark"));
    spans = stq.getPayloadSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 0, null);


    SpanQuery[] clauses = new SpanQuery[3];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 12, false);

    spans = spanNearQuery.getPayloadSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 2, new int[]{3,3});

     
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));

    spanNearQuery = new SpanNearQuery(clauses, 6, true);
   
    
    spans = spanNearQuery.getPayloadSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 1, new int[]{3});
     
    clauses = new SpanQuery[2];
     
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));

    spanNearQuery = new SpanNearQuery(clauses, 6, true);
     
   
    SpanQuery[] clauses2 = new SpanQuery[2];
     
    clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
    clauses2[1] = spanNearQuery;
     
    SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses2, 6, false);

    spans = nestedSpanNearQuery.getPayloadSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 2, new int[]{3,3});
  }
  
  public void testFirstClauseWithoutPayload() throws Exception {
    PayloadSpans spans;
    IndexSearcher searcher = getSearcher();

    SpanQuery[] clauses = new SpanQuery[3];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "nopayload"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "qq"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "ss"));

    SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 6, true);
    
    SpanQuery[] clauses2 = new SpanQuery[2];
     
    clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "pp"));
    clauses2[1] = spanNearQuery;

    SpanNearQuery snq = new SpanNearQuery(clauses2, 6, false);
    
    SpanQuery[] clauses3 = new SpanQuery[2];
     
    clauses3[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "np"));
    clauses3[1] = snq;
     
    SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses3, 6, false);

    spans = nestedSpanNearQuery.getPayloadSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 1, new int[]{3});
  }
  
  public void testHeavilyNestedSpanQuery() throws Exception {
    PayloadSpans spans;
    IndexSearcher searcher = getSearcher();

    SpanQuery[] clauses = new SpanQuery[3];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "two"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "three"));

    SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 5, true);
   
    clauses = new SpanQuery[3];
    clauses[0] = spanNearQuery; 
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "five"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "six"));

    SpanNearQuery spanNearQuery2 = new SpanNearQuery(clauses, 6, true);
     
    SpanQuery[] clauses2 = new SpanQuery[2];
    clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "eleven"));
    clauses2[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "ten"));
    SpanNearQuery spanNearQuery3 = new SpanNearQuery(clauses2, 2, false);
    
    SpanQuery[] clauses3 = new SpanQuery[3];
    clauses3[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "nine"));
    clauses3[1] = spanNearQuery2;
    clauses3[2] = spanNearQuery3;
     
    SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses3, 6, false);

    spans = nestedSpanNearQuery.getPayloadSpans(searcher.getIndexReader());
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 2, new int[]{8, 8});
  }
  
  public void testPayloadSpanUtil() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    PayloadAnalyzer analyzer = new PayloadAnalyzer();
    String[] docs = new String[]{};
    IndexWriter writer = new IndexWriter(directory, analyzer, true);
    writer.setSimilarity(similarity);
    Document doc = new Document();
    doc.add(new Field(PayloadHelper.FIELD,"xx rr yy mm  pp", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
  
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);

    IndexReader reader = searcher.getIndexReader();
    PayloadSpanUtil psu = new PayloadSpanUtil(reader);
    
    Collection payloads = psu.getPayloadsForQuery(new TermQuery(new Term(PayloadHelper.FIELD, "rr")));
    if(DEBUG)
      System.out.println("Num payloads:" + payloads.size());
    Iterator it = payloads.iterator();
    while(it.hasNext()) {
      byte[] bytes = (byte[]) it.next();
      if(DEBUG)
        System.out.println(new String(bytes));
    }
    
  }

  private void checkSpans(PayloadSpans spans, int expectedNumSpans, int expectedNumPayloads,
                          int expectedPayloadLength, int expectedFirstByte) throws IOException {
    assertTrue("spans is null and it shouldn't be", spans != null);
    //each position match should have a span associated with it, since there is just one underlying term query, there should
    //only be one entry in the span
    int seen = 0;
    while (spans.next() == true)
    {
      //if we expect payloads, then isPayloadAvailable should be true
      if (expectedNumPayloads > 0) {
        assertTrue("isPayloadAvailable is not returning the correct value: " + spans.isPayloadAvailable()
                + " and it should be: " + (expectedNumPayloads >  0),
                spans.isPayloadAvailable() == true);
      } else {
        assertTrue("isPayloadAvailable should be false", spans.isPayloadAvailable() == false);
      }
      //See payload helper, for the PayloadHelper.FIELD field, there is a single byte payload at every token
      if (spans.isPayloadAvailable()) {
        Collection payload = spans.getPayload();
        assertTrue("payload Size: " + payload.size() + " is not: " + expectedNumPayloads, payload.size() == expectedNumPayloads);
        for (Iterator iterator = payload.iterator(); iterator.hasNext();) {
           byte[] thePayload = (byte[]) iterator.next();
          assertTrue("payload[0] Size: " + thePayload.length + " is not: " + expectedPayloadLength,
                  thePayload.length == expectedPayloadLength);
          assertTrue(thePayload[0] + " does not equal: " + expectedFirstByte, thePayload[0] == expectedFirstByte);

        }

      }
      seen++;
    }
    assertTrue(seen + " does not equal: " + expectedNumSpans, seen == expectedNumSpans);
  }
  

  public void testShrinkToAfterShortestMatch() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new TestPayloadAnalyzer(),
        IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content", new StringReader("a b c d e f g h i j a k")));
    writer.addDocument(doc);
    writer.close();

    IndexSearcher is = new IndexSearcher(directory);

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 1, true);
    PayloadSpans spans = snq.getPayloadSpans(is.getIndexReader());

    TopDocs topDocs = is.search(snq, 1);
    Set payloadSet = new HashSet();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.next()) {
        Collection payloads = spans.getPayload();

        for (Iterator it = payloads.iterator(); it.hasNext();) {
          payloadSet.add(new String((byte[]) it.next()));
        }
      }
    }
    assertEquals(2, payloadSet.size());
    assertTrue(payloadSet.contains("a:Noise:10"));
    assertTrue(payloadSet.contains("k:Noise:11"));
  }
  
  public void testShrinkToAfterShortestMatch2() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new TestPayloadAnalyzer(),
        IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content", new StringReader("a b a d k f a h i k a k")));
    writer.addDocument(doc);
    writer.close();

    IndexSearcher is = new IndexSearcher(directory);

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 0, true);
    PayloadSpans spans = snq.getPayloadSpans(is.getIndexReader());

    TopDocs topDocs = is.search(snq, 1);
    Set payloadSet = new HashSet();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.next()) {
        Collection payloads = spans.getPayload();
        int cnt = 0;
        for (Iterator it = payloads.iterator(); it.hasNext();) {
          payloadSet.add(new String((byte[]) it.next()));
        }
      }
    }
    assertEquals(2, payloadSet.size());
    assertTrue(payloadSet.contains("a:Noise:10"));
    assertTrue(payloadSet.contains("k:Noise:11"));
  }
  
  public void testShrinkToAfterShortestMatch3() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new TestPayloadAnalyzer(),
        IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content", new StringReader("j k a l f k k p a t a k l k t a")));
    writer.addDocument(doc);
    writer.close();

    IndexSearcher is = new IndexSearcher(directory);

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 0, true);
    PayloadSpans spans = snq.getPayloadSpans(is.getIndexReader());

    TopDocs topDocs = is.search(snq, 1);
    Set payloadSet = new HashSet();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.next()) {
        Collection payloads = spans.getPayload();

        for (Iterator it = payloads.iterator(); it.hasNext();) {
          payloadSet.add(new String((byte[]) it.next()));
        }
      }
    }
    assertEquals(2, payloadSet.size());
    if(DEBUG) {
      Iterator pit = payloadSet.iterator();
      while (pit.hasNext()) {
        System.out.println("match:" + pit.next());
      }
    }
    assertTrue(payloadSet.contains("a:Noise:10"));
    assertTrue(payloadSet.contains("k:Noise:11"));
  }
  
  private IndexSearcher getSearcher() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    PayloadAnalyzer analyzer = new PayloadAnalyzer();
    String[] docs = new String[]{"xx rr yy mm  pp","xx yy mm rr pp", "nopayload qq ss pp np", "one two three four five six seven eight nine ten eleven", "nine one two three four five six seven eight eleven ten"};
    IndexWriter writer = new IndexWriter(directory, analyzer, true);

    writer.setSimilarity(similarity);

    Document doc = null;
    for(int i = 0; i < docs.length; i++) {
      doc = new Document();
      String docText = docs[i];
      doc.add(new Field(PayloadHelper.FIELD,docText, Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }

    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);
    return searcher;
  }
  
  private void checkSpans(PayloadSpans spans, int numSpans, int[] numPayloads) throws IOException {
    int cnt = 0;

    while (spans.next() == true) {
      if(DEBUG)
        System.out.println("\nSpans Dump --");
      if (spans.isPayloadAvailable()) {
        Collection payload = spans.getPayload();
        if(DEBUG)
          System.out.println("payloads for span:" + payload.size());
        Iterator it = payload.iterator();
        while(it.hasNext()) {
          byte[] bytes = (byte[]) it.next();
          if(DEBUG)
            System.out.println("doc:" + spans.doc() + " s:" + spans.start() + " e:" + spans.end() + " "
              + new String(bytes));
        }

        assertEquals(numPayloads[cnt],payload.size());
      } else {
        assertFalse("Expected spans:" + numPayloads[cnt] + " found: 0",numPayloads.length > 0 && numPayloads[cnt] > 0 );
      }
      cnt++;
    }

    assertEquals(numSpans, cnt);
  }

  class PayloadAnalyzer extends Analyzer {

    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new LowerCaseTokenizer(reader);
      result = new PayloadFilter(result, fieldName);
      return result;
    }
  }

  class PayloadFilter extends TokenFilter {
    String fieldName;
    int numSeen = 0;
    Set entities = new HashSet();
    Set nopayload = new HashSet();
    int pos;

    public PayloadFilter(TokenStream input, String fieldName) {
      super(input);
      this.fieldName = fieldName;
      pos = 0;
      entities.add("xx");
      entities.add("one");
      nopayload.add("nopayload");
      nopayload.add("np");

    }

    public Token next() throws IOException {
      Token result = input.next();
      if (result != null) {
        String token = new String(result.termBuffer(), 0, result.termLength());

        if (!nopayload.contains(token)) {
          if (entities.contains(token)) {
            result.setPayload(new Payload((token + ":Entity:"+ pos ).getBytes()));
          } else {
            result.setPayload(new Payload((token + ":Noise:" + pos ).getBytes()));
          }
        }
        pos += result.getPositionIncrement();
      }
      return result;
    }
  }
  
  public class TestPayloadAnalyzer extends Analyzer {

    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new LowerCaseTokenizer(reader);
      result = new PayloadFilter(result, fieldName);
      return result;
    }
  }
}
