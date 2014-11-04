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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.document.Document2;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests {@link PrefixQuery} class.
 *
 */
public class TestPrefixQuery extends LuceneTestCase {
  public void testPrefixQuery() throws Exception {
    Directory directory = newDirectory();

    String[] categories = new String[] {"/Computers",
                                        "/Computers/Mac",
                                        "/Computers/Windows"};
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < categories.length; i++) {
      Document2 doc = writer.newDocument();
      doc.addAtom("category", categories[i]);
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();

    PrefixQuery query = new PrefixQuery(new Term("category", "/Computers"));
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("All documents in /Computers category and below", 3, hits.length);

    query = new PrefixQuery(new Term("category", "/Computers/Mac"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("One in /Computers/Mac", 1, hits.length);

    query = new PrefixQuery(new Term("category", ""));
    Terms terms = MultiFields.getTerms(searcher.getIndexReader(), "category");
    assertFalse(query.getTermsEnum(terms) instanceof PrefixTermsEnum);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("everything", 3, hits.length);
    writer.close();
    reader.close();
    directory.close();
  }

  /** Make sure auto prefix terms are used with PrefixQuery. */
  public void testAutoPrefixTermsKickIn() throws Exception {

    List<String> prefixes = new ArrayList<>();
    for(int i=1;i<5;i++) {
      char[] chars = new char[i];
      Arrays.fill(chars, 'a');
      prefixes.add(new String(chars));
    }

    Set<String> randomTerms = new HashSet<>();
    int numTerms = atLeast(10000);
    while (randomTerms.size() < numTerms) {
      for(String prefix : prefixes) {
        randomTerms.add(prefix + TestUtil.randomRealisticUnicodeString(random()));
      }
    }

    int actualCount = 0;
    for(String term : randomTerms) {
      if (term.startsWith("aa")) {
        actualCount++;
      }
    }

    //System.out.println("actual count " + actualCount);

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    int minTermsInBlock = TestUtil.nextInt(random(), 2, 100);
    int maxTermsInBlock = Math.max(2, (minTermsInBlock-1)*2 + random().nextInt(100));

    // As long as this is never > actualCount, aa should always see at least one auto-prefix term:
    int minTermsAutoPrefix = TestUtil.nextInt(random(), 2, actualCount);
    int maxTermsAutoPrefix = random().nextBoolean() ? Math.max(2, (minTermsAutoPrefix-1)*2 + random().nextInt(100)) : Integer.MAX_VALUE;

    iwc.setCodec(TestUtil.alwaysPostingsFormat(new Lucene50PostingsFormat(minTermsInBlock, maxTermsInBlock,
                                                                          minTermsAutoPrefix, maxTermsAutoPrefix)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (String term : randomTerms) {
      Document2 doc = w.newDocument();
      doc.addAtom("field", term);
      w.addDocument(doc);
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    final Terms terms = MultiFields.getTerms(r, "field");
    IndexSearcher s = new IndexSearcher(r);
    final int finalActualCount = actualCount;
    PrefixQuery q = new PrefixQuery(new Term("field", "aa")) {
      public PrefixQuery checkTerms() throws IOException {
        TermsEnum termsEnum = getTermsEnum(terms, new AttributeSource());
        int count = 0;
        while (termsEnum.next() != null) {
          //System.out.println("got term: " + termsEnum.term().utf8ToString());
          count++;
        }

        // Auto-prefix term(s) should have kicked in, so we should have visited fewer than the total number of aa* terms:
        assertTrue(count < finalActualCount);

        return this;
      }
    }.checkTerms();

    int x = BooleanQuery.getMaxClauseCount();
    try {
      BooleanQuery.setMaxClauseCount(randomTerms.size());
      if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
      } else if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
      }

      assertEquals(actualCount, s.search(q, 1).totalHits);
    } finally {
      BooleanQuery.setMaxClauseCount(x);
    }

    r.close();
    w.close();
    dir.close();
  }
}
