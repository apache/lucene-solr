package org.apache.lucene.search;

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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Term position unit test.
 *
 *
 * @version $Revision$
 */
public class TestPositionIncrement extends LuceneTestCase {

  public void testSetPosition() throws Exception {
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new TokenStream() {
          private final String[] TOKENS = {"1", "2", "3", "4", "5"};
          private final int[] INCREMENTS = {1, 2, 1, 0, 1};
          private int i = 0;

          public Token next(final Token reusableToken) {
            assert reusableToken != null;
            if (i == TOKENS.length)
              return null;
            reusableToken.reinit(TOKENS[i], i, i);
            reusableToken.setPositionIncrement(INCREMENTS[i]);
            i++;
            return reusableToken;
          }
        };
      }
    };
    RAMDirectory store = new RAMDirectory();
    IndexWriter writer = new IndexWriter(store, analyzer, true,
                                         IndexWriter.MaxFieldLength.LIMITED);
    Document d = new Document();
    d.add(new Field("field", "bogus", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d);
    writer.optimize();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(store);
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

    // analyzer to introduce stopwords and increment gaps 
    Analyzer stpa = new Analyzer() {
      final WhitespaceAnalyzer a = new WhitespaceAnalyzer();
      public TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream ts = a.tokenStream(fieldName,reader);
        return new StopFilter(ts,new String[]{"stop"});
      }
    };

    // should not find "1 2" because there is a gap of 1 in the index
    QueryParser qp = new QueryParser("field",stpa);
    q = (PhraseQuery) qp.parse("\"1 2\"");
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // omitted stop word cannot help because stop filter swallows the increments. 
    q = (PhraseQuery) qp.parse("\"1 stop 2\"");
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // query parser alone won't help, because stop filter swallows the increments. 
    qp.setEnablePositionIncrements(true);
    q = (PhraseQuery) qp.parse("\"1 stop 2\"");
    hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    boolean dflt = StopFilter.getEnablePositionIncrementsDefault();
    try {
      // stop filter alone won't help, because query parser swallows the increments. 
      qp.setEnablePositionIncrements(false);
      StopFilter.setEnablePositionIncrementsDefault(true);
      q = (PhraseQuery) qp.parse("\"1 stop 2\"");
      hits = searcher.search(q, null, 1000).scoreDocs;
      assertEquals(0, hits.length);
      
      // when both qp qnd stopFilter propagate increments, we should find the doc.
      qp.setEnablePositionIncrements(true);
      q = (PhraseQuery) qp.parse("\"1 stop 2\"");
      hits = searcher.search(q, null, 1000).scoreDocs;
      assertEquals(1, hits.length);
    } finally {
      StopFilter.setEnablePositionIncrementsDefault(dflt);
    }
  }

  /**
   * Basic analyzer behavior should be to keep sequential terms in one
   * increment from one another.
   */
  public void testIncrementingPositions() throws Exception {
    Analyzer analyzer = new WhitespaceAnalyzer();
    TokenStream ts = analyzer.tokenStream("field",
                                new StringReader("one two three four five"));
    final Token reusableToken = new Token();
    for (Token nextToken = ts.next(reusableToken); nextToken != null; nextToken = ts.next(reusableToken)) {
      assertEquals(nextToken.term(), 1, nextToken.getPositionIncrement());
    }
  }
}
