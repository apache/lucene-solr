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
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.payloads.PayloadHelper;
import org.apache.lucene.search.payloads.PayloadSpanUtil;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestPayloadSpans extends LuceneTestCase {
  private IndexSearcher searcher;
  private Similarity similarity = new DefaultSimilarity();
  protected IndexReader indexReader;
  private IndexReader closeIndexReader;
  private Directory directory;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    PayloadHelper helper = new PayloadHelper();
    searcher = helper.setUp(random(), similarity, 1000);
    indexReader = searcher.getIndexReader();
  }

  public void testSpanTermQuery() throws Exception {
    SpanTermQuery stq;
    Spans spans;
    stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "seventy"));
    spans = MultiSpansWrapper.wrap(indexReader.getContext(), stq);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 100, 1, 1, 1);

    stq = new SpanTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "seventy"));  
    spans = MultiSpansWrapper.wrap(indexReader.getContext(), stq);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 100, 0, 0, 0);
  }

  public void testSpanFirst() throws IOException {

    SpanQuery match;
    SpanFirstQuery sfq;
    match = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    sfq = new SpanFirstQuery(match, 2);
    Spans spans = MultiSpansWrapper.wrap(indexReader.getContext(), sfq);
    checkSpans(spans, 109, 1, 1, 1);
    //Test more complicated subclause
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "hundred"));
    match = new SpanNearQuery(clauses, 0, true);
    sfq = new SpanFirstQuery(match, 2);
    checkSpans(MultiSpansWrapper.wrap(indexReader.getContext(), sfq), 100, 2, 1, 1);

    match = new SpanNearQuery(clauses, 0, false);
    sfq = new SpanFirstQuery(match, 2);
    checkSpans(MultiSpansWrapper.wrap(indexReader.getContext(), sfq), 100, 2, 1, 1);
    
  }
  
  public void testSpanNot() throws Exception {
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "three"));
    SpanQuery spq = new SpanNearQuery(clauses, 5, true);
    SpanNotQuery snq = new SpanNotQuery(spq, new SpanTermQuery(new Term(PayloadHelper.FIELD, "two")));



    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(TEST_VERSION_CURRENT, new PayloadAnalyzer()).setSimilarity(similarity));

    Document doc = new Document();
    doc.add(newTextField(PayloadHelper.FIELD, "one two three one four three", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();
    

    checkSpans(MultiSpansWrapper.wrap(reader.getContext(), snq), 1,new int[]{2});
    reader.close();
    directory.close();
  }
  
  public void testNestedSpans() throws Exception {
    SpanTermQuery stq;
    Spans spans;
    IndexSearcher searcher = getSearcher();
    stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "mark"));
    spans = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), stq);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 0, null);


    SpanQuery[] clauses = new SpanQuery[3];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 12, false);

    spans = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), spanNearQuery);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 2, new int[]{3,3});

     
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));

    spanNearQuery = new SpanNearQuery(clauses, 6, true);
   
    spans = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), spanNearQuery);

    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 1, new int[]{3});
     
    clauses = new SpanQuery[2];
     
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));

    spanNearQuery = new SpanNearQuery(clauses, 6, true);
     
    // xx within 6 of rr
    
    SpanQuery[] clauses2 = new SpanQuery[2];
     
    clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
    clauses2[1] = spanNearQuery;
     
    SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses2, 6, false);
    
    // yy within 6 of xx within 6 of rr

    spans = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), nestedSpanNearQuery);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 2, new int[]{3,3});
    closeIndexReader.close();
    directory.close();
  }
  
  public void testFirstClauseWithoutPayload() throws Exception {
    Spans spans;
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
    spans = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), nestedSpanNearQuery);

    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 1, new int[]{3});
    closeIndexReader.close();
    directory.close();
  }
  
  public void testHeavilyNestedSpanQuery() throws Exception {
    Spans spans;
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

    spans = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), nestedSpanNearQuery);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, 2, new int[]{8, 8});
    closeIndexReader.close();
    directory.close();
  }
  
  public void testShrinkToAfterShortestMatch() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(TEST_VERSION_CURRENT, new TestPayloadAnalyzer()));

    Document doc = new Document();
    doc.add(new TextField("content", new StringReader("a b c d e f g h i j a k")));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();
    IndexSearcher is = newSearcher(reader);
    writer.close();

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 1, true);
    Spans spans = MultiSpansWrapper.wrap(is.getTopReaderContext(), snq);

    TopDocs topDocs = is.search(snq, 1);
    Set<String> payloadSet = new HashSet<String>();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.next()) {
        Collection<byte[]> payloads = spans.getPayload();

        for (final byte [] payload : payloads) {
          payloadSet.add(new String(payload, "UTF-8"));
        }
      }
    }
    assertEquals(2, payloadSet.size());
    assertTrue(payloadSet.contains("a:Noise:10"));
    assertTrue(payloadSet.contains("k:Noise:11"));
    reader.close();
    directory.close();
  }
  
  public void testShrinkToAfterShortestMatch2() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(TEST_VERSION_CURRENT, new TestPayloadAnalyzer()));

    Document doc = new Document();
    doc.add(new TextField("content", new StringReader("a b a d k f a h i k a k")));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    IndexSearcher is = newSearcher(reader);
    writer.close();

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 0, true);
    Spans spans =  MultiSpansWrapper.wrap(is.getTopReaderContext(), snq);

    TopDocs topDocs = is.search(snq, 1);
    Set<String> payloadSet = new HashSet<String>();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.next()) {
        Collection<byte[]> payloads = spans.getPayload();
        for (final byte[] payload : payloads) {
          payloadSet.add(new String(payload, "UTF-8"));
        }
      }
    }
    assertEquals(2, payloadSet.size());
    assertTrue(payloadSet.contains("a:Noise:10"));
    assertTrue(payloadSet.contains("k:Noise:11"));
    reader.close();
    directory.close();
  }
  
  public void testShrinkToAfterShortestMatch3() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(TEST_VERSION_CURRENT, new TestPayloadAnalyzer()));

    Document doc = new Document();
    doc.add(new TextField("content", new StringReader("j k a l f k k p a t a k l k t a")));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    IndexSearcher is = newSearcher(reader);
    writer.close();

    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 0, true);
    Spans spans =  MultiSpansWrapper.wrap(is.getTopReaderContext(), snq);

    TopDocs topDocs = is.search(snq, 1);
    Set<String> payloadSet = new HashSet<String>();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.next()) {
        Collection<byte[]> payloads = spans.getPayload();

        for (final byte [] payload : payloads) {
          payloadSet.add(new String(payload, "UTF-8"));
        }
      }
    }
    assertEquals(2, payloadSet.size());
    if(VERBOSE) {
      for (final String payload : payloadSet)
        System.out.println("match:" +  payload);
      
    }
    assertTrue(payloadSet.contains("a:Noise:10"));
    assertTrue(payloadSet.contains("k:Noise:11"));
    reader.close();
    directory.close();
  }
  
  public void testPayloadSpanUtil() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(TEST_VERSION_CURRENT, new PayloadAnalyzer()).setSimilarity(similarity));

    Document doc = new Document();
    doc.add(newTextField(PayloadHelper.FIELD, "xx rr yy mm  pp", Field.Store.YES));
    writer.addDocument(doc);
  
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    PayloadSpanUtil psu = new PayloadSpanUtil(searcher.getTopReaderContext());
    
    Collection<byte[]> payloads = psu.getPayloadsForQuery(new TermQuery(new Term(PayloadHelper.FIELD, "rr")));
    if(VERBOSE)
      System.out.println("Num payloads:" + payloads.size());
    for (final byte [] bytes : payloads) {
      if(VERBOSE)
        System.out.println(new String(bytes, "UTF-8"));
    }
    reader.close();
    directory.close();
  }

  private void checkSpans(Spans spans, int expectedNumSpans, int expectedNumPayloads,
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
        Collection<byte[]> payload = spans.getPayload();
        assertTrue("payload Size: " + payload.size() + " is not: " + expectedNumPayloads, payload.size() == expectedNumPayloads);
        for (final byte [] thePayload : payload) {
          assertTrue("payload[0] Size: " + thePayload.length + " is not: " + expectedPayloadLength,
                  thePayload.length == expectedPayloadLength);
          assertTrue(thePayload[0] + " does not equal: " + expectedFirstByte, thePayload[0] == expectedFirstByte);

        }

      }
      seen++;
    }
    assertTrue(seen + " does not equal: " + expectedNumSpans, seen == expectedNumSpans);
  }
  
  private IndexSearcher getSearcher() throws Exception {
    directory = newDirectory();
    String[] docs = new String[]{"xx rr yy mm  pp","xx yy mm rr pp", "nopayload qq ss pp np", "one two three four five six seven eight nine ten eleven", "nine one two three four five six seven eight eleven ten"};
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(TEST_VERSION_CURRENT, new PayloadAnalyzer()).setSimilarity(similarity));

    Document doc = null;
    for(int i = 0; i < docs.length; i++) {
      doc = new Document();
      String docText = docs[i];
      doc.add(newTextField(PayloadHelper.FIELD, docText, Field.Store.YES));
      writer.addDocument(doc);
    }

    closeIndexReader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(closeIndexReader);
    return searcher;
  }
  
  private void checkSpans(Spans spans, int numSpans, int[] numPayloads) throws IOException {
    int cnt = 0;

    while (spans.next() == true) {
      if(VERBOSE)
        System.out.println("\nSpans Dump --");
      if (spans.isPayloadAvailable()) {
        Collection<byte[]> payload = spans.getPayload();
        if(VERBOSE)
          System.out.println("payloads for span:" + payload.size());
        for (final byte [] bytes : payload) {
          if(VERBOSE)
            System.out.println("doc:" + spans.doc() + " s:" + spans.start() + " e:" + spans.end() + " "
              + new String(bytes, "UTF-8"));
        }

        assertEquals(numPayloads[cnt],payload.size());
      } else {
        assertFalse("Expected spans:" + numPayloads[cnt] + " found: 0",numPayloads.length > 0 && numPayloads[cnt] > 0 );
      }
      cnt++;
    }

    assertEquals(numSpans, cnt);
  }

  final class PayloadAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer result = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result));
    }
  }

  final class PayloadFilter extends TokenFilter {
    Set<String> entities = new HashSet<String>();
    Set<String> nopayload = new HashSet<String>();
    int pos;
    PayloadAttribute payloadAtt;
    CharTermAttribute termAtt;
    PositionIncrementAttribute posIncrAtt;

    public PayloadFilter(TokenStream input) {
      super(input);
      pos = 0;
      entities.add("xx");
      entities.add("one");
      nopayload.add("nopayload");
      nopayload.add("np");
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      payloadAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        String token = termAtt.toString();

        if (!nopayload.contains(token)) {
          if (entities.contains(token)) {
            payloadAtt.setPayload(new BytesRef(token + ":Entity:"+ pos ));
          } else {
            payloadAtt.setPayload(new BytesRef(token + ":Noise:" + pos ));
          }
        }
        pos += posIncrAtt.getPositionIncrement();
        return true;
      }
      return false;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.pos = 0;
    }
  }
  
  public final class TestPayloadAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer result = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result));
    }
  }
}
