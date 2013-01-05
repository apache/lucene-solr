package org.apache.lucene.search.postingshighlight;

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
import java.text.BreakIterator;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

@SuppressCodecs({"MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom"})
public class TestPostingsHighlighterRanking extends LuceneTestCase {
  /** 
   * indexes a bunch of gibberish, and then highlights top(n).
   * asserts that top(n) highlights is a subset of top(n+1) up to some max N
   */
  // TODO: this only tests single-valued fields. we should also index multiple values per field!
  public void testRanking() throws Exception {
    // number of documents: we will check each one
    final int numDocs = atLeast(100);
    // number of top-N snippets, we will check 1 .. N
    final int maxTopN = 5;
    // maximum number of elements to put in a sentence.
    final int maxSentenceLength = 10;
    // maximum number of sentences in a document
    final int maxNumSentences = 20;
    
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    Document document = new Document();
    Field id = new StringField("id", "", Field.Store.NO);
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    document.add(id);
    document.add(body);
    
    for (int i = 0; i < numDocs; i++) {;
      StringBuilder bodyText = new StringBuilder();
      int numSentences = _TestUtil.nextInt(random(), 1, maxNumSentences);
      for (int j = 0; j < numSentences; j++) {
        bodyText.append(newSentence(random(), maxSentenceLength));
      }
      body.setStringValue(bodyText.toString());
      id.setStringValue(Integer.toString(i));
      iw.addDocument(document);
    }
    
    IndexReader ir = iw.getReader();
    IndexSearcher searcher = newSearcher(ir);
    for (int i = 0; i < numDocs; i++) {
      checkDocument(searcher, i, maxTopN);
    }
    iw.close();
    ir.close();
    dir.close();
  }
  
  private void checkDocument(IndexSearcher is, int doc, int maxTopN) throws IOException {
    for (int ch = 'a'; ch <= 'z'; ch++) {
      Term term = new Term("body", "" + (char)ch);
      // check a simple term query
      checkQuery(is, new TermQuery(term), doc, maxTopN);
      // check a boolean query
      BooleanQuery bq = new BooleanQuery();
      bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
      Term nextTerm = new Term("body", "" + (char)(ch+1));
      bq.add(new TermQuery(nextTerm), BooleanClause.Occur.SHOULD);
      checkQuery(is, bq, doc, maxTopN);
    }
  }
  
  private void checkQuery(IndexSearcher is, Query query, int doc, int maxTopN) throws IOException {
    for (int n = 1; n < maxTopN; n++) {
      FakePassageFormatter f1 = new FakePassageFormatter();
      PostingsHighlighter p1 = new PostingsHighlighter(Integer.MAX_VALUE-1, 
                                                       BreakIterator.getSentenceInstance(Locale.ROOT), 
                                                       new PassageScorer(),
                                                       f1);
      FakePassageFormatter f2 = new FakePassageFormatter();
      PostingsHighlighter p2 = new PostingsHighlighter(Integer.MAX_VALUE-1, 
                                                       BreakIterator.getSentenceInstance(Locale.ROOT), 
                                                       new PassageScorer(),
                                                       f2);
      BooleanQuery bq = new BooleanQuery(false);
      bq.add(query, BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("id", Integer.toString(doc))), BooleanClause.Occur.MUST);
      TopDocs td = is.search(bq, 1);
      p1.highlight("body", bq, is, td, n);
      p2.highlight("body", bq, is, td, n+1);
      assertTrue(f2.seen.containsAll(f1.seen));
    }
  }
  
  /** 
   * returns a new random sentence, up to maxSentenceLength "words" in length.
   * each word is a single character (a-z). The first one is capitalized.
   */
  private String newSentence(Random r, int maxSentenceLength) {
    StringBuilder sb = new StringBuilder();
    int numElements = _TestUtil.nextInt(r, 1, maxSentenceLength);
    for (int i = 0; i < numElements; i++) {
      if (sb.length() > 0) {
        sb.append(' ');
        sb.append((char)_TestUtil.nextInt(r, 'a', 'z'));
      } else {
        // capitalize the first word to help breakiterator
        sb.append((char)_TestUtil.nextInt(r, 'A', 'Z'));
      }
    }
    sb.append(". "); // finalize sentence
    return sb.toString();
  }
  
  /** 
   * a fake formatter that doesn't actually format passages.
   * instead it just collects them for asserts!
   */
  static class FakePassageFormatter extends PassageFormatter {
    HashSet<Pair> seen = new HashSet<Pair>();
    
    @Override
    public String format(Passage passages[], String content) {
      for (Passage p : passages) {
        // verify some basics about the passage
        assertTrue(p.getScore() >= 0);
        assertTrue(p.getNumMatches() > 0);
        assertTrue(p.getStartOffset() >= 0);
        assertTrue(p.getStartOffset() <= content.length());
        // we use a very simple analyzer. so we can assert the matches are correct
        for (int i = 0; i < p.getNumMatches(); i++) {
          Term term = p.getMatchTerms()[i];
          assertEquals("body", term.field());
          int matchStart = p.getMatchStarts()[i];
          assertTrue(matchStart >= 0);
          int matchEnd = p.getMatchEnds()[i];
          assertTrue(matchEnd >= 0);
          // single character terms
          assertEquals(matchStart+1, matchEnd);
          // and the offsets must be correct...
          BytesRef bytes = term.bytes();
          assertEquals(1, bytes.length);
          assertEquals((char)bytes.bytes[bytes.offset], Character.toLowerCase(content.charAt(matchStart)));
        }
        // record just the start/end offset for simplicity
        seen.add(new Pair(p.getStartOffset(), p.getEndOffset()));
      }
      return "bogus!!!!!!";
    }
  }
  
  static class Pair {
    final int start;
    final int end;
    
    Pair(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + end;
      result = prime * result + start;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Pair other = (Pair) obj;
      if (end != other.end) {
        return false;
      }
      if (start != other.start) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "Pair [start=" + start + ", end=" + end + "]";
    }
  }
}
