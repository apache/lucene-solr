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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.MultiSpansWrapper;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
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
    PayloadSpanCollector collector = new PayloadSpanCollector();
    spans = MultiSpansWrapper.wrap(indexReader, stq, SpanWeight.Postings.PAYLOADS);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 100, 1, 1, 1);

    stq = new SpanTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "seventy"));  
    spans = MultiSpansWrapper.wrap(indexReader, stq, SpanWeight.Postings.PAYLOADS);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 100, 0, 0, 0);
  }

  public void testSpanFirst() throws IOException {

    SpanQuery match;
    SpanFirstQuery sfq;
    match = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    sfq = new SpanFirstQuery(match, 2);
    PayloadSpanCollector collector = new PayloadSpanCollector();
    Spans spans = MultiSpansWrapper.wrap(indexReader, sfq, SpanWeight.Postings.PAYLOADS);
    checkSpans(spans, collector, 109, 1, 1, 1);
    //Test more complicated subclause
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "hundred"));
    match = new SpanNearQuery(clauses, 0, true);
    sfq = new SpanFirstQuery(match, 2);
    checkSpans(MultiSpansWrapper.wrap(indexReader, sfq, SpanWeight.Postings.PAYLOADS), collector, 100, 2, 1, 1);

    match = new SpanNearQuery(clauses, 0, false);
    sfq = new SpanFirstQuery(match, 2);
    checkSpans(MultiSpansWrapper.wrap(indexReader, sfq, SpanWeight.Postings.PAYLOADS), collector, 100, 2, 1, 1);
    
  }
  
  public void testSpanNot() throws Exception {
    SpanQuery[] clauses = new SpanQuery[2];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "three"));
    SpanQuery spq = new SpanNearQuery(clauses, 5, true);
    SpanNotQuery snq = new SpanNotQuery(spq, new SpanTermQuery(new Term(PayloadHelper.FIELD, "two")));



    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(new PayloadAnalyzer()).setSimilarity(similarity));

    Document doc = new Document();
    doc.add(newTextField(PayloadHelper.FIELD, "one two three one four three", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    PayloadSpanCollector collector = new PayloadSpanCollector();
    checkSpans(MultiSpansWrapper.wrap(reader, snq, SpanWeight.Postings.PAYLOADS), collector, 1, new int[]{2});
    reader.close();
    directory.close();
  }
  
  public void testNestedSpans() throws Exception {
    SpanTermQuery stq;
    Spans spans;
    IndexSearcher searcher = getSearcher();
    PayloadSpanCollector collector = new PayloadSpanCollector();

    stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "mark"));
    spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), stq, SpanWeight.Postings.PAYLOADS);
    assertNull(spans);

    SpanQuery[] clauses = new SpanQuery[3];
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 12, false);

    spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), spanNearQuery, SpanWeight.Postings.PAYLOADS);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 2, new int[]{3,3});

     
    clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
    clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
    clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));

    spanNearQuery = new SpanNearQuery(clauses, 6, true);
   
    spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), spanNearQuery, SpanWeight.Postings.PAYLOADS);

    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 1, new int[]{3});
     
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
    spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), nestedSpanNearQuery, SpanWeight.Postings.PAYLOADS);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 2, new int[]{3,3});
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

    PayloadSpanCollector collector = new PayloadSpanCollector();
    SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses3, 6, false);
    spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), nestedSpanNearQuery, SpanWeight.Postings.PAYLOADS);

    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 1, new int[]{3});
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

    PayloadSpanCollector collector = new PayloadSpanCollector();
    spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), nestedSpanNearQuery, SpanWeight.Postings.PAYLOADS);
    assertTrue("spans is null and it shouldn't be", spans != null);
    checkSpans(spans, collector, 2, new int[]{8, 8});
    closeIndexReader.close();
    directory.close();
  }
  
  public void testShrinkToAfterShortestMatch() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(new TestPayloadAnalyzer()));

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
    PayloadSpanCollector collector = new PayloadSpanCollector();
    Spans spans = MultiSpansWrapper.wrap(is.getIndexReader(), snq, SpanWeight.Postings.PAYLOADS);

    TopDocs topDocs = is.search(snq, 1);
    Set<String> payloadSet = new HashSet<>();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
        while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
          collector.reset();
          spans.collect(collector);
          Collection<byte[]> payloads = collector.getPayloads();
          for (final byte [] payload : payloads) {
            payloadSet.add(new String(payload, StandardCharsets.UTF_8));
          }
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
                                                     newIndexWriterConfig(new TestPayloadAnalyzer()));

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
    PayloadSpanCollector collector = new PayloadSpanCollector();
    Spans spans =  MultiSpansWrapper.wrap(is.getIndexReader(), snq, SpanWeight.Postings.PAYLOADS);

    TopDocs topDocs = is.search(snq, 1);
    Set<String> payloadSet = new HashSet<>();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
        while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
          collector.reset();
          spans.collect(collector);
          Collection<byte[]> payloads = collector.getPayloads();
  
          for (final byte [] payload : payloads) {
            payloadSet.add(new String(payload, StandardCharsets.UTF_8));
          }
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
                                                     newIndexWriterConfig(new TestPayloadAnalyzer()));

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
    PayloadSpanCollector collector = new PayloadSpanCollector();
    Spans spans =  MultiSpansWrapper.wrap(is.getIndexReader(), snq, SpanWeight.Postings.PAYLOADS);

    TopDocs topDocs = is.search(snq, 1);
    Set<String> payloadSet = new HashSet<>();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
        while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
          collector.reset();
          spans.collect(collector);
          Collection<byte[]> payloads = collector.getPayloads();
  
          for (final byte [] payload : payloads) {
            payloadSet.add(new String(payload, StandardCharsets.UTF_8));
          }
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
                                                     newIndexWriterConfig(new PayloadAnalyzer()).setSimilarity(similarity));

    Document doc = new Document();
    doc.add(newTextField(PayloadHelper.FIELD, "xx rr yy mm  pp", Field.Store.YES));
    writer.addDocument(doc);
  
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    PayloadSpanUtil psu = new PayloadSpanUtil(searcher.getTopReaderContext());
    
    Collection<byte[]> payloads = psu.getPayloadsForQuery(new TermQuery(new Term(PayloadHelper.FIELD, "rr")));
    if(VERBOSE) {
      System.out.println("Num payloads:" + payloads.size());
      for (final byte [] bytes : payloads) {
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
      }
    }
    reader.close();
    directory.close();
  }

  private void checkSpans(Spans spans, PayloadSpanCollector collector, int expectedNumSpans, int expectedNumPayloads,
                          int expectedPayloadLength, int expectedFirstByte) throws IOException {
    assertTrue("spans is null and it shouldn't be", spans != null);
    //each position match should have a span associated with it, since there is just one underlying term query, there should
    //only be one entry in the span
    int seen = 0;
    while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
      while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        collector.reset();
        spans.collect(collector);

        Collection<byte[]> payload = collector.getPayloads();
        assertEquals("payload size", expectedNumPayloads, payload.size());
        for (final byte [] thePayload : payload) {
          assertEquals("payload length", expectedPayloadLength, thePayload.length);
          assertEquals("payload first byte", expectedFirstByte, thePayload[0]);
        }

        seen++;
      }
    }
    assertEquals("expectedNumSpans", expectedNumSpans, seen);
  }
  
  private IndexSearcher getSearcher() throws Exception {
    directory = newDirectory();
    String[] docs = new String[]{"xx rr yy mm  pp","xx yy mm rr pp", "nopayload qq ss pp np", "one two three four five six seven eight nine ten eleven", "nine one two three four five six seven eight eleven ten"};
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
                                                     newIndexWriterConfig(new PayloadAnalyzer()).setSimilarity(similarity));

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
  
  private void checkSpans(Spans spans, PayloadSpanCollector collector, int numSpans, int[] numPayloads) throws IOException {
    int cnt = 0;

    while (spans.nextDoc() != Spans.NO_MORE_DOCS) {
      while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        if(VERBOSE)
          System.out.println("\nSpans Dump --");
        collector.reset();
        spans.collect(collector);

        Collection<byte[]> payload = collector.getPayloads();
        if(VERBOSE) {
          System.out.println("payloads for span:" + payload.size());
          for (final byte [] bytes : payload) {
            System.out.println("doc:" + spans.docID() + " s:" + spans.startPosition() + " e:" + spans.endPosition() + " "
                + new String(bytes, StandardCharsets.UTF_8));
          }
        }
        assertEquals("payload size", numPayloads[cnt], payload.size());

        cnt++;
      }
    }

    assertEquals("expected numSpans", numSpans, cnt);
  }

  final class PayloadAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result));
    }
  }

  final class PayloadFilter extends TokenFilter {
    Set<String> entities = new HashSet<>();
    Set<String> nopayload = new HashSet<>();
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
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new PayloadFilter(result));
    }
  }
}
