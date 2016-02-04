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
package org.apache.lucene.search.postingshighlight;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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
    
    for (int i = 0; i < numDocs; i++) {
      StringBuilder bodyText = new StringBuilder();
      int numSentences = TestUtil.nextInt(random(), 1, maxNumSentences);
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
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
      Term nextTerm = new Term("body", "" + (char)(ch+1));
      bq.add(new TermQuery(nextTerm), BooleanClause.Occur.SHOULD);
      checkQuery(is, bq.build(), doc, maxTopN);
    }
  }
  
  private void checkQuery(IndexSearcher is, Query query, int doc, int maxTopN) throws IOException {
    for (int n = 1; n < maxTopN; n++) {
      final FakePassageFormatter f1 = new FakePassageFormatter();
      PostingsHighlighter p1 = new PostingsHighlighter(Integer.MAX_VALUE-1) {
          @Override
          protected PassageFormatter getFormatter(String field) {
            assertEquals("body", field);
            return f1;
          }
        };

      final FakePassageFormatter f2 = new FakePassageFormatter();
      PostingsHighlighter p2 = new PostingsHighlighter(Integer.MAX_VALUE-1) {
          @Override
          protected PassageFormatter getFormatter(String field) {
            assertEquals("body", field);
            return f2;
          }
        };

      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(query, BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("id", Integer.toString(doc))), BooleanClause.Occur.MUST);
      TopDocs td = is.search(bq.build(), 1);
      p1.highlight("body", bq.build(), is, td, n);
      p2.highlight("body", bq.build(), is, td, n+1);
      assertTrue(f2.seen.containsAll(f1.seen));
    }
  }
  
  /** 
   * returns a new random sentence, up to maxSentenceLength "words" in length.
   * each word is a single character (a-z). The first one is capitalized.
   */
  private String newSentence(Random r, int maxSentenceLength) {
    StringBuilder sb = new StringBuilder();
    int numElements = TestUtil.nextInt(r, 1, maxSentenceLength);
    for (int i = 0; i < numElements; i++) {
      if (sb.length() > 0) {
        sb.append(' ');
        sb.append((char) TestUtil.nextInt(r, 'a', 'z'));
      } else {
        // capitalize the first word to help breakiterator
        sb.append((char) TestUtil.nextInt(r, 'A', 'Z'));
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
    HashSet<Pair> seen = new HashSet<>();
    
    @Override
    public String format(Passage passages[], String content) {
      for (Passage p : passages) {
        // verify some basics about the passage
        assertTrue(p.getScore() >= 0);
        assertTrue(p.getNumMatches() > 0);
        assertTrue(p.getStartOffset() >= 0);
        assertTrue(p.getStartOffset() <= content.length());
        assertTrue(p.getEndOffset() >= p.getStartOffset());
        assertTrue(p.getEndOffset() <= content.length());
        // we use a very simple analyzer. so we can assert the matches are correct
        int lastMatchStart = -1;
        for (int i = 0; i < p.getNumMatches(); i++) {
          BytesRef term = p.getMatchTerms()[i];
          int matchStart = p.getMatchStarts()[i];
          assertTrue(matchStart >= 0);
          // must at least start within the passage
          assertTrue(matchStart < p.getEndOffset());
          int matchEnd = p.getMatchEnds()[i];
          assertTrue(matchEnd >= 0);
          // always moving forward
          assertTrue(matchStart >= lastMatchStart);
          lastMatchStart = matchStart;
          // single character terms
          assertEquals(matchStart+1, matchEnd);
          // and the offsets must be correct...
          assertEquals(1, term.length);
          assertEquals((char)term.bytes[term.offset], Character.toLowerCase(content.charAt(matchStart)));
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
  
  /** sets b=0 to disable passage length normalization */
  public void testCustomB() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test.  This test is a better test but the sentence is excruiatingly long, " + 
                        "you have no idea how painful it was for me to type this long sentence into my IDE.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter(10000) {
        @Override
        protected PassageScorer getScorer(String field) {
          return new PassageScorer(1.2f, 0, 87);
        }
      };
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 1);
    assertEquals(1, snippets.length);
    assertTrue(snippets[0].startsWith("This <b>test</b> is a better <b>test</b>"));
    
    ir.close();
    dir.close();
  }
  
  /** sets k1=0 for simple coordinate-level match (# of query terms present) */
  public void testCustomK1() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This has only foo foo. " + 
                        "On the other hand this sentence contains both foo and bar. " + 
                        "This has only bar bar bar bar bar bar bar bar bar bar bar bar.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter(10000) {
        @Override
        protected PassageScorer getScorer(String field) {
          return new PassageScorer(0, 0.75f, 87);
        }
      };
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("body", "foo")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "bar")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query.build(), 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query.build(), searcher, topDocs, 1);
    assertEquals(1, snippets.length);
    assertTrue(snippets[0].startsWith("On the other hand"));
    
    ir.close();
    dir.close();
  }
}
