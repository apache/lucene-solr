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
package org.apache.lucene.concordance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.concordance.classic.AbstractConcordanceWindowCollector;
import org.apache.lucene.concordance.classic.ConcordanceSearcher;
import org.apache.lucene.concordance.classic.ConcordanceSortOrder;
import org.apache.lucene.concordance.classic.ConcordanceWindow;
import org.apache.lucene.concordance.classic.DocIdBuilder;
import org.apache.lucene.concordance.classic.DocMetadataExtractor;
import org.apache.lucene.concordance.classic.WindowBuilder;
import org.apache.lucene.concordance.classic.impl.ConcordanceWindowCollector;
import org.apache.lucene.concordance.classic.impl.DedupingConcordanceWindowCollector;
import org.apache.lucene.concordance.classic.impl.DefaultSortKeyBuilder;
import org.apache.lucene.concordance.classic.impl.IndexIdDocIdBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConcordanceSearcher extends ConcordanceTestBase {

  private final static DocMetadataExtractor metadataExtractor =
      new DocMetadataExtractor() {
        private final Set<String> fields = new HashSet<>();
        private final Map<String, String> data = new HashMap<>();

        @Override
        public Set<String> getFieldSelector() {
          return fields;
        }

        @Override
        public Map<String, String> extract(Document d) {
          return data;
        }
      };

  private final static DocIdBuilder docIdBuilder = new IndexIdDocIdBuilder();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // NOOP for now
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // NOOP for now
  }

  @Test
  public void testSimple() throws Exception {
    String[] docs = new String[]{"a b c a b c", "c b a c b a"};
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);

    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    WindowBuilder wb = new WindowBuilder(10, 10,
        analyzer.getOffsetGap(FIELD),
        new DefaultSortKeyBuilder(ConcordanceSortOrder.PRE), metadataExtractor, docIdBuilder);
    ConcordanceSearcher searcher = new ConcordanceSearcher(wb);
    SpanQuery q = new SpanTermQuery(new Term(FIELD, "a"));

    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(3);
    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);

    assertEquals(3, collector.size());

    collector = new ConcordanceWindowCollector(ConcordanceWindowCollector.COLLECT_ALL);
    searcher.search(indexSearcher, FIELD, q, null, analyzer, collector);

    // test result size
    assertEquals(4, collector.size());

    // test result with sort order = pre
    List<ConcordanceWindow> windows = collector.getSortedWindows();
    String[] pres = new String[]{"", "c b", "c b a c b", "a b c"};
    String[] posts = new String[]{" b c a b c", " c b a", "", " b c"};

    for (int i = 0; i < windows.size(); i++) {
      ConcordanceWindow w = windows.get(i);

      assertEquals(pres[i], w.getPre());
      assertEquals(posts[i], w.getPost());
    }

    // test sort order post
    // sort key is built at search time, so must re-search
    wb = new WindowBuilder(10, 10,
        analyzer.getOffsetGap(FIELD),
        new DefaultSortKeyBuilder(ConcordanceSortOrder.POST), metadataExtractor, docIdBuilder);
    searcher = new ConcordanceSearcher(wb);

    collector = new ConcordanceWindowCollector(ConcordanceWindowCollector.COLLECT_ALL);
    searcher.search(indexSearcher, FIELD, q,
        null, analyzer, collector);

    windows = collector.getSortedWindows();

    posts = new String[]{"", " b c", " b c a b c", " c b a",};
    for (int i = 0; i < windows.size(); i++) {
      ConcordanceWindow w = windows.get(i);
      assertEquals(posts[i], w.getPost());
    }
    reader.close();
    directory.close();
  }

  @Test
  public void testSimpleMultiValuedField() throws Exception {
    String[] doc = new String[]{"a b c a b c", "c b a c b a"};
    List<String[]> docs = new ArrayList<>();
    docs.add(doc);
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    ConcordanceSearcher searcher = new ConcordanceSearcher(
        new WindowBuilder(10, 10, analyzer.getOffsetGap(FIELD)));
    SpanQuery q = new SpanTermQuery(new Term(FIELD, "a"));

    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(100);

    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);

    // test result size
    assertEquals(4, collector.size());

    // test result with sort order = pre
    List<ConcordanceWindow> windows = collector.getSortedWindows();
    String[] pres = new String[]{"", "c b", "c b a c b", "a b c"};
    String[] posts = new String[]{" b c a b c", " c b a", "", " b c"};

    for (int i = 0; i < pres.length; i++) {
      ConcordanceWindow w = windows.get(i);

      assertEquals("pres: " + i, pres[i], w.getPre());

      assertEquals("posts: " + i, posts[i], w.getPost());
    }

    // test sort order post
    // sort key is built at search time, so must re-search
    WindowBuilder wb = new WindowBuilder(10, 10,
        analyzer.getOffsetGap(FIELD),
        new DefaultSortKeyBuilder(ConcordanceSortOrder.POST), metadataExtractor, docIdBuilder);
    searcher = new ConcordanceSearcher(wb);

    collector = new ConcordanceWindowCollector(100);

    searcher.search(indexSearcher, FIELD, q, null, analyzer, collector);

    windows = collector.getSortedWindows();

    posts = new String[]{"", " b c", " b c a b c", " c b a",};
    for (int i = 0; i < posts.length; i++) {
      ConcordanceWindow w = windows.get(i);
      assertEquals(posts[i], w.getPost());
    }
    reader.close();
    directory.close();
  }

  @Test
  public void testWindowLengths() throws Exception {
    String[] doc = new String[]{"a b c d e f g"};
    List<String[]> docs = new ArrayList<>();
    docs.add(doc);
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);

    SpanQuery q = new SpanTermQuery(new Term(FIELD, "d"));

    String[] pres = {"", "c", "b c", "a b c", "a b c", "a b c"};
    String[] posts = {"", " e", " e f", " e f g", " e f g", " e f g"};

    for (int tokensBefore = 0; tokensBefore < pres.length; tokensBefore++) {
      for (int tokensAfter = 0; tokensAfter < posts.length; tokensAfter++) {
        WindowBuilder wb = new WindowBuilder(tokensBefore, tokensAfter,
            analyzer.getOffsetGap(FIELD));
        ConcordanceSearcher searcher = new ConcordanceSearcher(wb);
        ConcordanceWindowCollector collector = new ConcordanceWindowCollector(100);
        searcher.search(indexSearcher, FIELD, q, null, analyzer, collector);
        ConcordanceWindow w = collector.getSortedWindows().get(0);
        assertEquals(tokensBefore + " : " + tokensAfter, pres[tokensBefore], w.getPre());
        assertEquals(tokensBefore + " : " + tokensAfter, posts[tokensAfter], w.getPost());
      }
    }

    reader.close();
    directory.close();

  }

  @Test
  public void testClockworkOrangeMultiValuedFieldProblem() throws Exception {
    /*
     * test handling of target match (or not) over different indices into multivalued
     * field array
     */
    String[] doc = new String[]{"a b c a b the", "clockwork",
        "orange b a c b a"};
    List<String[]> docs = new ArrayList<>();
    docs.add(doc);
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET, 0, 10);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    WindowBuilder wb = new WindowBuilder(3, 3, analyzer.getOffsetGap(FIELD));


    ConcordanceSearcher searcher = new ConcordanceSearcher(wb);
    SpanQuery q1 = new SpanTermQuery(
        new Term(FIELD, "the"));
    SpanQuery q2 = new SpanTermQuery(new Term(FIELD,
        "clockwork"));
    SpanQuery q3 = new SpanTermQuery(new Term(FIELD,
        "orange"));
    SpanQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 3, true);
    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(3);

    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);
    assertEquals(1, collector.size());

    ConcordanceWindow w = collector.getSortedWindows().iterator().next();
    assertEquals("target", "the | clockwork | orange", w.getTarget());
    assertEquals("pre", "c a b", w.getPre());
    assertEquals("post", " b a c", w.getPost());

    reader.close();
    directory.close();

    // test hit even over long inter-field gap
    analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET, 20, 50);
    directory = getDirectory(analyzer, docs);
    reader = DirectoryReader.open(directory);
    indexSearcher = new IndexSearcher(reader);

    wb = new WindowBuilder(3, 3, analyzer.getOffsetGap(FIELD));

    searcher = new ConcordanceSearcher(wb);
    q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 120, true);
    collector = new ConcordanceWindowCollector(100);

    searcher.search(indexSearcher, FIELD, q, null, analyzer, collector);

    assertEquals(1, collector.size());
    w = collector.getSortedWindows().iterator().next();
    assertEquals("target", "the | clockwork | orange", w.getTarget());
    assertEquals("pre", "c a b", w.getPre());
    assertEquals("post", " b a c", w.getPost());

    reader.close();
    directory.close();
    // test miss
    analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET, 100, 100);
    directory = getDirectory(analyzer, docs);
    reader = DirectoryReader.open(directory);
    indexSearcher = new IndexSearcher(reader);

    wb = new WindowBuilder();
    searcher = new ConcordanceSearcher(wb);
    q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 5, true);
    collector = new ConcordanceWindowCollector(100);

    searcher.search(indexSearcher, FIELD, q, null, analyzer, collector);

    assertEquals(0, collector.size());

    reader.close();
    directory.close();
  }

  @Test
  public void testWithStops() throws Exception {
    String[] docs = new String[]{"a b the d e the f", "g h the d the j"};
    Analyzer analyzer = getAnalyzer(MockTokenFilter.ENGLISH_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    WindowBuilder wb = new WindowBuilder(2, 2, analyzer.getOffsetGap(FIELD));

    ConcordanceSearcher searcher = new ConcordanceSearcher(wb);
    SpanQuery q = new SpanTermQuery(new Term(FIELD, "d"));
    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(3);

    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);
    List<ConcordanceWindow> windows = collector.getSortedWindows();
    assertEquals(2, windows.size());

    // the second word after the target is a stop word
    // this post-component of this window should only go to the first word after
    // the target
    assertEquals("b the", windows.get(0).getPre());
    assertEquals("d", windows.get(0).getTarget());
    assertEquals(" e", windows.get(0).getPost());

    assertEquals("h the", windows.get(1).getPre());
    assertEquals("d", windows.get(1).getTarget());
    assertEquals(" the j", windows.get(1).getPost());


    reader.close();
    directory.close();
  }

  @Test
  public void testBasicStandardQueryConversion() throws Exception {
    String[] docs = new String[]{"a b c a b c", "c b a c b a d e a",
        "c b a c b a e a b c a"};
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    ConcordanceSearcher searcher = new ConcordanceSearcher(
        new WindowBuilder(10, 10, analyzer.getOffsetGap(FIELD)));
    BooleanQuery q = new BooleanQuery.Builder()
      .add(new TermQuery(new Term(FIELD, "a")), Occur.MUST)
      .add(new TermQuery(new Term(FIELD, "d")),
        Occur.MUST_NOT).build();

    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(10);
    searcher.search(indexSearcher,
        FIELD, q, null,
        analyzer, collector);
    // shouldn't include document with "d"
    assertEquals(6, collector.size());

    // should only include document with "e" and not "d"
    Query filter = new TermQuery(new Term(
        FIELD, "e"));
    collector = new ConcordanceWindowCollector(10);

    searcher.search(indexSearcher, FIELD, (Query) q, filter, analyzer, collector);
    assertEquals(4, collector.size());

    reader.close();
    directory.close();
  }

  @Test
  public void testMismatchingFieldsInStandardQueryConversion() throws Exception {
    // tests what happens if a Query doesn't contain a term in the "span" field
    // in the searcher...should be no exception and zero documents returned.

    String[] docs = new String[]{"a b c a b c",};
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);

    ConcordanceSearcher searcher = new ConcordanceSearcher(
        new WindowBuilder(10, 10, analyzer.getOffsetGap(FIELD)));

    Query q = new TermQuery(new Term("_" + FIELD, "a"));

    int windowCount = -1;
    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(10);

    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);
    windowCount = collector.size();
    assertEquals(0, windowCount);
    reader.close();
    directory.close();
  }

  @Test
  public void testUniqueCollector() throws Exception {
    String[] docs = new String[]{"a b c d c b a",
        "a B C d c b a",
        "a b C d C B a",
        "a b c d C B A",
        "e f g d g f e",
        "h i j d j i h"
    };

    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    ConcordanceSearcher searcher = new ConcordanceSearcher(
        new WindowBuilder(10, 10, analyzer.getOffsetGap(FIELD)));
    SpanQuery q = new SpanTermQuery(new Term(FIELD, "d"));

    DedupingConcordanceWindowCollector collector = new DedupingConcordanceWindowCollector(2);
    searcher.search(indexSearcher,
        FIELD, (Query) q, null,
        analyzer, collector);
    assertEquals(2, collector.size());


    collector =
        new DedupingConcordanceWindowCollector(AbstractConcordanceWindowCollector.COLLECT_ALL);
    searcher.search(indexSearcher,
        FIELD, (Query) q, null,
        analyzer, collector);
    assertEquals(3, collector.size());


    reader.close();
    directory.close();

  }


  @Test
  public void testUniqueCollectorWithSameWindowOverflow() throws Exception {
    String[] docs = new String[]{"a b c d c b a",
        "a b c d c b a",
        "a b c d c b a",
        "a b c d c b a",
        "e f g d g f e",
        "h i j d j i h"
    };

    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    ConcordanceSearcher searcher = new ConcordanceSearcher(
        new WindowBuilder(10, 10, analyzer.getOffsetGap(FIELD)));

    SpanQuery q = new SpanTermQuery(new Term(FIELD, "d"));

    DedupingConcordanceWindowCollector collector = new DedupingConcordanceWindowCollector(3);
    searcher.search(indexSearcher,
        FIELD, (Query) q, null,
        analyzer, collector);
    assertEquals(3, collector.size());
    assertEquals(4, collector.getSortedWindows().get(0).getCount());
    reader.close();
    directory.close();
  }

  @Test
  public void testAllowTargetOverlaps() throws Exception {
    String[] docs = new String[]{"a b c"};
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);

    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    WindowBuilder wb = new WindowBuilder(10, 10,
        analyzer.getOffsetGap(FIELD),
        new DefaultSortKeyBuilder(ConcordanceSortOrder.PRE), metadataExtractor, docIdBuilder);
    ConcordanceSearcher searcher = new ConcordanceSearcher(wb);
    SpanQuery term = new SpanTermQuery(new Term(FIELD, "a"));
    SpanQuery phrase = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "a")),
            new SpanTermQuery(new Term(FIELD, "b"))
        }, 0, true);
    SpanOrQuery q = new SpanOrQuery(
        new SpanQuery[]{
            term,
            phrase
        }
    );

    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(10);
    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);

    //default should be: don't allow target overlaps
    assertEquals(1, collector.size());

    searcher.setAllowTargetOverlaps(true);
    collector = new ConcordanceWindowCollector(10);
    searcher.search(indexSearcher, FIELD,
        q, null, analyzer, collector);

    //now there should be two windows with allowTargetOverlaps = true
    assertEquals(2, collector.size());
    reader.close();
    directory.close();
  }

  @Test
  public void testRewrites() throws Exception {
    //test to make sure that queries are rewritten
    //first test straight prefix queries
    String[] docs = new String[]{"aa ba ca aa ba ca", "ca ba aa ca ba aa da ea za",
        "ca ba aa ca ba aa ea aa ba ca za"};
    Analyzer analyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET);
    Directory directory = getDirectory(analyzer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    ConcordanceSearcher searcher = new ConcordanceSearcher(
        new WindowBuilder(10, 10, analyzer.getOffsetGap(FIELD)));
    BooleanQuery q = new BooleanQuery.Builder()
        .add(new PrefixQuery(new Term(FIELD, "a")), Occur.MUST)
        .add(new PrefixQuery(new Term(FIELD, "d")),
            Occur.MUST_NOT).build();

    //now test straight and span wrapper
    ConcordanceWindowCollector collector = new ConcordanceWindowCollector(10);
    searcher.search(indexSearcher,
        FIELD, q, new PrefixQuery(new Term(FIELD, "z")),
        analyzer, collector);
    // shouldn't include document with "da", but must include one with za
    assertEquals(3, collector.size());

    collector = new ConcordanceWindowCollector(10);
    searcher.search(indexSearcher,
        FIELD, q, new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(FIELD, "z"))),
        analyzer, collector);
    // shouldn't include document with "da", but must include one with za
    assertEquals(3, collector.size());

    reader.close();
    directory.close();
  }

}
