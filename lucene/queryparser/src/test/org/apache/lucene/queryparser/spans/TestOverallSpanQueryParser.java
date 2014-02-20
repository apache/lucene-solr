package org.apache.lucene.queryparser.spans;

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.spans.SpanQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestOverallSpanQueryParser extends LuceneTestCase {
  private final static String FIELD1 = "f1";
  private final static String FIELD2 = "f2";
  private static Analyzer analyzer = null;
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  private static SpanQueryParser parser;

  @BeforeClass
  public static void beforeClass() throws Exception {
    analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE,
            false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer)
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));
    String[] f1Docs = new String[] { 
        "quick brown AND fox",//0
        "quick brown AND dog", //1
        "quick brown dog", //2
        "whan that aprile with its shoures perced", //3
        "its shoures pierced", //4
        "its shoures perced", //5
        "#####", //before asterisk  //6
        "&&&&&", //after asterisk for range query //7
        "ab*de", //8
        "abcde", //9
        "blah disco fever blah", //10
        "blah bieber fever blah", //11
        "blah dengue fever blah", //12
        "blah saturday night fever with john travolta" //13

    };
    String [] f2Docs = new String[] {
        "zero",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen"
    };
    for (int i = 0; i < f1Docs.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(FIELD1, f1Docs[i], Field.Store.YES));
      doc.add(newTextField(FIELD2, f2Docs[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();

    parser = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, analyzer);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    reader = null;
    searcher = null;
    directory = null;
    analyzer = null;
  }

  public void testComplexQueries() throws Exception {
    //complex span not 
    compareHits("+f1:[fever (bieber [jo*n travlota~1] disc*)]!~2,5 +f2:(ten eleven twelve thirteen)", 12);
    compareHits("+f1:[fever (bieber [jo*n travlota~1] disc*)]!~2,5 -f2:(ten eleven twelve thirteen)");
    compareHits("+f1:[fever (bieber [travlota~1 jo*n]~>3 disc*)]!~2,5 +f2:(ten eleven twelve thirteen)", 12, 13);
    compareHits("+f1:[fever (bieber [jo*n travlota~1]~>3 disc*)]!~2,5 +f2:(ten eleven twelve thirteen)", 12);
    compareHits("-f1:[fever (bieber [jo*n travlota~1]~>3 disc*)]!~2,5 +f2:(ten eleven twelve thirteen)", 10, 11, 13);
  }

  public void testNegativeOnly() throws Exception {
    //negative only queries
    compareHits("-fever", 0,1,2,3,4,5,6,7,8,9);
    compareHits("-f1:fever", 0,1,2,3,4,5,6,7,8,9);
    compareHits("-fever -brown", 3,4,5,6,7,8,9);
  }

  public void testUnlimitedRange() throws Exception {
    //just make sure that -1 is interpreted as infinity
    parser.setSpanNearMaxDistance(-1);
    parser.setPhraseSlop(0);
    compareHits("[quick dog]~10", 1, 2);
    parser.setSpanNearMaxDistance(100);

  }

  public void testBooleanQueryConstruction() throws Exception {
    String s = "cat dog AND elephant aardvark";
    Query q = parser.parse(s);
    assertTrue(q instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery)q;
    List<BooleanClause> clauses = bq.clauses();
    assertEquals(4, clauses.size());
    testForClause(clauses, "cat", Occur.SHOULD);
    testForClause(clauses, "dog", Occur.MUST);
    testForClause(clauses, "elephant", Occur.MUST);
    testForClause(clauses, "aardvark", Occur.SHOULD);

    s = "cat dog NOT elephant aardvark";
    q = parser.parse(s);
    assertTrue(q instanceof BooleanQuery);
    bq = (BooleanQuery)q;
    clauses = bq.clauses();
    assertEquals(4, clauses.size());
    testForClause(clauses, "cat", Occur.SHOULD);
    testForClause(clauses, "dog", Occur.SHOULD);
    testForClause(clauses, "elephant", Occur.MUST_NOT);
    testForClause(clauses, "aardvark", Occur.SHOULD);

    s = "cat +dog -elephant +aardvark";
    q = parser.parse(s);
    assertTrue(q instanceof BooleanQuery);
    bq = (BooleanQuery)q;
    clauses = bq.clauses();
    assertEquals(4, clauses.size());
    testForClause(clauses, "cat", Occur.SHOULD);
    testForClause(clauses, "dog", Occur.MUST);
    testForClause(clauses, "elephant", Occur.MUST_NOT);
    testForClause(clauses, "aardvark", Occur.MUST);
  }

  public void testFields() throws Exception {
    compareHits("f1:brown f2:three", 0, 1, 2, 3);

    //four should go back to f1
    compareHits("f1:brown f2:three four", 0, 1, 2, 3);
    compareHits("f1:brown f2:(three four)", 0, 1, 2, 3, 4);
    compareHits("f1:brown f2:(three four) five", 0, 1, 2, 3, 4);
    compareHits("f1:brown f2:(three four) f2:five", 0, 1, 2, 3, 4, 5);
    compareHits("f1:brown f2:(f1:three four) f2:five", 0, 1, 2, 4, 5);

    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD2, analyzer);
    compareHits(p, "f1:brown three four", 0, 1, 2, 3, 4);
    compareHits(p, "f1:brown (three four)", 0, 1, 2, 3, 4);
    compareHits(p, "f1:brown (three four) five", 0, 1, 2, 3, 4, 5);
    compareHits(p, "f1:brown (three four) five", 0, 1, 2, 3, 4, 5);
    compareHits(p, "f1:brown (f1:three four) five", 0, 1, 2, 4, 5);
  }

  public void testBooleanOrHits() throws Exception {
    compareHits("f2:three (brown dog)", 0, 1, 2, 3);
    compareHits("f2:three (brown dog)~2", 1, 2, 3);
  } 

  public void testBooleanHits() throws Exception {
    //test treatment of AND within phrase
    compareHits("quick NOT [brown AND (fox dog)]", 2);
    compareHits("quick AND [bruwn~1 AND (f?x do?)]", 0, 1);
    compareHits("(whan AND aprile) (shoures NOT perced)", 3, 4);
    //test escaping of AND
    compareHits("zoo \\AND elephant", 0, 1);
  }

  private void testForClause(List<BooleanClause> clauses, String term, Occur occur) {
    assertTrue(clauses.contains(
        new BooleanClause(
            new SpanTermQuery(
                new Term(FIELD1, term)), 
                occur))
        );
  }
  
  private void compareHits(String s, int ... docids ) throws Exception{
    compareHits(new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, analyzer), s, docids);
  }

  private void compareHits(SpanQueryParser p, String s, int ... docids ) throws Exception{
    Query q = p.parse(s);
    TopScoreDocCollector results = TopScoreDocCollector.create(1000, true);
    searcher.search(q, results);
    ScoreDoc[] scoreDocs = results.topDocs().scoreDocs;
    Set<Integer> hits = new HashSet<Integer>();

    for (int i = 0; i < scoreDocs.length; i++) {
      hits.add(scoreDocs[i].doc);
    }
    assertEquals(docids.length, hits.size());

    for (int i = 0; i < docids.length; i++) {
      assertTrue("couldn't find " + Integer.toString(docids[i]) + " among the hits", hits.contains(docids[i]));
    }
  }

  public void testExceptions() {
    String[] strings = new String[]{
        "cat OR OR dog",
        "cat OR AND dog",
        "cat AND AND dog",
        "cat NOT NOT dog",
        "cat NOT AND dog",
        "cat NOT OR dog",
        "cat NOT -dog",
        "cat NOT +dog",
        "OR",
        "+",
        "AND dog",
        "OR dog",
        "dog AND",
        "dog OR",
        "dog NOT",
        "dog -",
    "dog +"};

    for (String s : strings) {
      testException(s, parser);
    }
  }

  private void testException(String s, SpanQueryParser p) {
    try {
      p.parse(s);
      fail("didn't get expected exception");
    } catch (ParseException expected) {}
  }

  public void testIsEscaped() throws Exception{
    String[] notEscaped = new String[]{
        "abcd",
        "a\\\\d",
    };
    for (String s : notEscaped) {
      assertFalse(s, SpanQueryParserBase.isCharEscaped(s, 3));
    }
    String[] escaped = new String[]{
        "ab\\d",
        "\\\\\\d",
    };
    for (String s : escaped) {
      assertTrue(s, SpanQueryParserBase.isCharEscaped(s, 3));
    }

    Query q = parser.parse("abc\\~2.0");
    assertTrue(q.toString(), q instanceof SpanTermQuery);
    q = parser.parse("abc\\\\\\~2.0");
    assertTrue(q.toString(), q instanceof SpanTermQuery);
    q = parser.parse("abc\\\\~2.0");
    assertTrue(q.toString(), q instanceof SpanMultiTermQueryWrapper);

    q = parser.parse("abc\\*d");
    assertTrue(q.toString(), q instanceof SpanTermQuery);

    q = parser.parse("abc\\\\\\*d");
    assertTrue(q.toString(), q instanceof SpanTermQuery);

    q = parser.parse("abc\\\\*d");
    assertTrue(q.toString(), q instanceof SpanMultiTermQueryWrapper);
  }
}
