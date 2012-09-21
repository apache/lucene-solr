package org.apache.lucene.search;

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
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.search.payloads.PayloadSpanUtil;
import org.apache.lucene.search.spans.MultiSpansWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Term position unit test.
 *
 *
 */
public class TestPositionIncrement extends LuceneTestCase {

  final static boolean VERBOSE = false;

  public void testSetPosition() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new Tokenizer(reader) {
          // TODO: use CannedTokenStream
          private final String[] TOKENS = {"1", "2", "3", "4", "5"};
          private final int[] INCREMENTS = {1, 2, 1, 0, 1};
          private int i = 0;

          PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
          CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
          OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
          
          @Override
          public boolean incrementToken() {
            if (i == TOKENS.length)
              return false;
            clearAttributes();
            termAtt.append(TOKENS[i]);
            offsetAtt.setOffset(i,i);
            posIncrAtt.setPositionIncrement(INCREMENTS[i]);
            i++;
            return true;
          }

          @Override
          public void reset() throws IOException {
            super.reset();
            this.i = 0;
          }
        });
      }
    };
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), store, analyzer);
    Document d = new Document();
    d.add(newTextField("field", "bogus", Field.Store.YES));
    writer.addDocument(d);
    IndexReader reader = writer.getReader();
    writer.close();
    

    IndexSearcher searcher = newSearcher(reader);
    
    DocsAndPositionsEnum pos = MultiFields.getTermPositionsEnum(searcher.getIndexReader(),
                                                                MultiFields.getLiveDocs(searcher.getIndexReader()),
                                                                "field",
                                                                new BytesRef("1"));
    pos.nextDoc();
    // first token should be at position 0
    assertEquals(0, pos.nextPosition());
    
    pos = MultiFields.getTermPositionsEnum(searcher.getIndexReader(),
                                           MultiFields.getLiveDocs(searcher.getIndexReader()),
                                           "field",
                                           new BytesRef("2"));
    pos.nextDoc();
    // second token should be at position 2
    assertEquals(2, pos.nextPosition());
    
    PhraseQuery q;
    ScoreDoc[] hits;

    q = new PhraseQuery();
    q.add(new Term("field", "1"));
    q.add(new Term("field", "2"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // same as previous, just specify positions explicitely.
    q = new PhraseQuery(); 
    q.add(new Term("field", "1"),0);
    q.add(new Term("field", "2"),1);
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // specifying correct positions should find the phrase.
    q = new PhraseQuery();
    q.add(new Term("field", "1"),0);
    q.add(new Term("field", "2"),2);
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery();
    q.add(new Term("field", "2"));
    q.add(new Term("field", "3"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery();
    q.add(new Term("field", "3"));
    q.add(new Term("field", "4"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // phrase query would find it when correct positions are specified. 
    q = new PhraseQuery();
    q.add(new Term("field", "3"),0);
    q.add(new Term("field", "4"),0);
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // phrase query should fail for non existing searched term 
    // even if there exist another searched terms in the same searched position. 
    q = new PhraseQuery();
    q.add(new Term("field", "3"),0);
    q.add(new Term("field", "9"),0);
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // multi-phrase query should succed for non existing searched term
    // because there exist another searched terms in the same searched position. 
    MultiPhraseQuery mq = new MultiPhraseQuery();
    mq.add(new Term[]{new Term("field", "3"),new Term("field", "9")},0);
    hits = searcher.search(mq, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery();
    q.add(new Term("field", "2"));
    q.add(new Term("field", "4"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery();
    q.add(new Term("field", "3"));
    q.add(new Term("field", "5"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery();
    q.add(new Term("field", "4"));
    q.add(new Term("field", "5"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    q = new PhraseQuery();
    q.add(new Term("field", "2"));
    q.add(new Term("field", "5"));
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    reader.close();
    store.close();
  }

  public void testPayloadsPos0() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new MockPayloadAnalyzer());
    Document doc = new Document();
    doc.add(new TextField("content", new StringReader(
        "a a b c d e a f g h i j a b k k")));
    writer.addDocument(doc);

    final IndexReader readerFromWriter = writer.getReader();
    AtomicReader r = SlowCompositeReaderWrapper.wrap(readerFromWriter);

    DocsAndPositionsEnum tp = r.termPositionsEnum(new Term("content", "a"));
    
    int count = 0;
    assertTrue(tp.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    // "a" occurs 4 times
    assertEquals(4, tp.freq());
    assertEquals(0, tp.nextPosition());
    assertEquals(1, tp.nextPosition());
    assertEquals(3, tp.nextPosition());
    assertEquals(6, tp.nextPosition());

    // only one doc has "a"
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, tp.nextDoc());

    IndexSearcher is = newSearcher(readerFromWriter);
  
    SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
    SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
    SpanQuery[] sqs = { stq1, stq2 };
    SpanNearQuery snq = new SpanNearQuery(sqs, 30, false);

    count = 0;
    boolean sawZero = false;
    if (VERBOSE) {
      System.out.println("\ngetPayloadSpans test");
    }
    Spans pspans = MultiSpansWrapper.wrap(is.getTopReaderContext(), snq);
    while (pspans.next()) {
      if (VERBOSE) {
        System.out.println("doc " + pspans.doc() + ": span " + pspans.start()
            + " to " + pspans.end());
      }
      Collection<byte[]> payloads = pspans.getPayload();
      sawZero |= pspans.start() == 0;
      for (byte[] bytes : payloads) {
        count++;
        if (VERBOSE) {
          System.out.println("  payload: " + new String(bytes, "UTF-8"));
        }
      }
    }
    assertTrue(sawZero);
    assertEquals(5, count);

    // System.out.println("\ngetSpans test");
    Spans spans = MultiSpansWrapper.wrap(is.getTopReaderContext(), snq);
    count = 0;
    sawZero = false;
    while (spans.next()) {
      count++;
      sawZero |= spans.start() == 0;
      // System.out.println(spans.doc() + " - " + spans.start() + " - " +
      // spans.end());
    }
    assertEquals(4, count);
    assertTrue(sawZero);

    // System.out.println("\nPayloadSpanUtil test");

    sawZero = false;
    PayloadSpanUtil psu = new PayloadSpanUtil(is.getTopReaderContext());
    Collection<byte[]> pls = psu.getPayloadsForQuery(snq);
    count = pls.size();
    for (byte[] bytes : pls) {
      String s = new String(bytes, "UTF-8");
      //System.out.println(s);
      sawZero |= s.equals("pos: 0");
    }
    assertEquals(5, count);
    assertTrue(sawZero);
    writer.close();
    is.getIndexReader().close();
    dir.close();
  }
}
