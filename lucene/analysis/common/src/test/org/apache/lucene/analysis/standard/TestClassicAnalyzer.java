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
package org.apache.lucene.analysis.standard;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

/** tests for classicanalyzer */
public class TestClassicAnalyzer extends BaseTokenStreamTestCase {

  private Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new ClassicAnalyzer();
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }

  public void testMaxTermLength() throws Exception {
    ClassicAnalyzer sa = new ClassicAnalyzer();
    sa.setMaxTokenLength(5);
    assertAnalyzesTo(sa, "ab cd toolong xy z", new String[]{"ab", "cd", "xy", "z"});
    sa.close();
  }

  public void testMaxTermLength2() throws Exception {
    ClassicAnalyzer sa = new ClassicAnalyzer();
    assertAnalyzesTo(sa, "ab cd toolong xy z", new String[]{"ab", "cd", "toolong", "xy", "z"});
    sa.setMaxTokenLength(5);
    
    assertAnalyzesTo(sa, "ab cd toolong xy z", new String[]{"ab", "cd", "xy", "z"}, new int[]{1, 1, 2, 1});
    sa.close();
  }

  public void testMaxTermLength3() throws Exception {
    char[] chars = new char[255];
    for(int i=0;i<255;i++)
      chars[i] = 'a';
    String longTerm = new String(chars, 0, 255);
    
    assertAnalyzesTo(a, "ab cd " + longTerm + " xy z", new String[]{"ab", "cd", longTerm, "xy", "z"});
    assertAnalyzesTo(a, "ab cd " + longTerm + "a xy z", new String[]{"ab", "cd", "xy", "z"});
  }

  public void testAlphanumeric() throws Exception {
    // alphanumeric tokens
    assertAnalyzesTo(a, "B2B", new String[]{"b2b"});
    assertAnalyzesTo(a, "2B", new String[]{"2b"});
  }

  public void testUnderscores() throws Exception {
    // underscores are delimiters, but not in email addresses (below)
    assertAnalyzesTo(a, "word_having_underscore", new String[]{"word", "having", "underscore"});
    assertAnalyzesTo(a, "word_with_underscore_and_stopwords", new String[]{"word", "underscore", "stopwords"});
  }

  public void testDelimiters() throws Exception {
    // other delimiters: "-", "/", ","
    assertAnalyzesTo(a, "some-dashed-phrase", new String[]{"some", "dashed", "phrase"});
    assertAnalyzesTo(a, "dogs,chase,cats", new String[]{"dogs", "chase", "cats"});
    assertAnalyzesTo(a, "ac/dc", new String[]{"ac", "dc"});
  }

  public void testApostrophes() throws Exception {
    // internal apostrophes: O'Reilly, you're, O'Reilly's
    // possessives are actually removed by StardardFilter, not the tokenizer
    assertAnalyzesTo(a, "O'Reilly", new String[]{"o'reilly"});
    assertAnalyzesTo(a, "you're", new String[]{"you're"});
    assertAnalyzesTo(a, "she's", new String[]{"she"});
    assertAnalyzesTo(a, "Jim's", new String[]{"jim"});
    assertAnalyzesTo(a, "don't", new String[]{"don't"});
    assertAnalyzesTo(a, "O'Reilly's", new String[]{"o'reilly"});
  }

  public void testTSADash() throws Exception {
    // t and s had been stopwords in Lucene <= 2.0, which made it impossible
    // to correctly search for these terms:
    assertAnalyzesTo(a, "s-class", new String[]{"s", "class"});
    assertAnalyzesTo(a, "t-com", new String[]{"t", "com"});
    // 'a' is still a stopword:
    assertAnalyzesTo(a, "a-class", new String[]{"class"});
  }

  public void testCompanyNames() throws Exception {
    // company names
    assertAnalyzesTo(a, "AT&T", new String[]{"at&t"});
    assertAnalyzesTo(a, "Excite@Home", new String[]{"excite@home"});
  }

  public void testLucene1140() throws Exception {
    try {
      ClassicAnalyzer analyzer = new ClassicAnalyzer();
      assertAnalyzesTo(analyzer, "www.nutch.org.", new String[]{ "www.nutch.org" }, new String[] { "<HOST>" });
      analyzer.close();
    } catch (NullPointerException e) {
      fail("Should not throw an NPE and it did");
    }

  }

  public void testDomainNames() throws Exception {
    // Current lucene should not show the bug
    ClassicAnalyzer a2 = new ClassicAnalyzer();

    // domain names
    assertAnalyzesTo(a2, "www.nutch.org", new String[]{"www.nutch.org"});
    //Notice the trailing .  See https://issues.apache.org/jira/browse/LUCENE-1068.
    // the following should be recognized as HOST:
    assertAnalyzesTo(a2, "www.nutch.org.", new String[]{ "www.nutch.org" }, new String[] { "<HOST>" });

    // 2.3 should show the bug. But, alas, it's obsolete, we don't support it.
    // a2 = new ClassicAnalyzer(org.apache.lucene.util.Version.LUCENE_23);
    // assertAnalyzesTo(a2, "www.nutch.org.", new String[]{ "wwwnutchorg" }, new String[] { "<ACRONYM>" });

    // 2.4 should not show the bug. But, alas, it's also obsolete,
    // so we check latest released (Robert's gonna break this on 4.0 soon :) )
    a2.close();
    a2 = new ClassicAnalyzer();
    assertAnalyzesTo(a2, "www.nutch.org.", new String[]{ "www.nutch.org" }, new String[] { "<HOST>" });
    a2.close();
  }

  public void testEMailAddresses() throws Exception {
    // email addresses, possibly with underscores, periods, etc
    assertAnalyzesTo(a, "test@example.com", new String[]{"test@example.com"});
    assertAnalyzesTo(a, "first.lastname@example.com", new String[]{"first.lastname@example.com"});
    assertAnalyzesTo(a, "first_lastname@example.com", new String[]{"first_lastname@example.com"});
  }

  public void testNumeric() throws Exception {
    // floating point, serial, model numbers, ip addresses, etc.
    // every other segment must have at least one digit
    assertAnalyzesTo(a, "21.35", new String[]{"21.35"});
    assertAnalyzesTo(a, "R2D2 C3PO", new String[]{"r2d2", "c3po"});
    assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
    assertAnalyzesTo(a, "1-2-3", new String[]{"1-2-3"});
    assertAnalyzesTo(a, "a1-b2-c3", new String[]{"a1-b2-c3"});
    assertAnalyzesTo(a, "a1-b-c3", new String[]{"a1-b-c3"});
  }

  public void testTextWithNumbers() throws Exception {
    // numbers
    assertAnalyzesTo(a, "David has 5000 bones", new String[]{"david", "has", "5000", "bones"});
  }

  public void testVariousText() throws Exception {
    // various
    assertAnalyzesTo(a, "C embedded developers wanted", new String[]{"c", "embedded", "developers", "wanted"});
    assertAnalyzesTo(a, "foo bar FOO BAR", new String[]{"foo", "bar", "foo", "bar"});
    assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", new String[]{"foo", "bar", "foo", "bar"});
    assertAnalyzesTo(a, "\"QUOTED\" word", new String[]{"quoted", "word"});
  }

  public void testAcronyms() throws Exception {
    // acronyms have their dots stripped
    assertAnalyzesTo(a, "U.S.A.", new String[]{"usa"});
  }

  public void testCPlusPlusHash() throws Exception {
    // It would be nice to change the grammar in StandardTokenizer.jj to make "C#" and "C++" end up as tokens.
    assertAnalyzesTo(a, "C++", new String[]{"c"});
    assertAnalyzesTo(a, "C#", new String[]{"c"});
  }

  public void testKorean() throws Exception {
    // Korean words
    assertAnalyzesTo(a, "안녕하세요 한글입니다", new String[]{"안녕하세요", "한글입니다"});
  }

  // Compliance with the "old" JavaCC-based analyzer, see:
  // https://issues.apache.org/jira/browse/LUCENE-966#action_12516752

  public void testComplianceFileName() throws Exception {
    assertAnalyzesTo(a, "2004.jpg",
            new String[]{"2004.jpg"},
            new String[]{"<HOST>"});
  }

  public void testComplianceNumericIncorrect() throws Exception {
    assertAnalyzesTo(a, "62.46",
            new String[]{"62.46"},
            new String[]{"<HOST>"});
  }

  public void testComplianceNumericLong() throws Exception {
    assertAnalyzesTo(a, "978-0-94045043-1",
            new String[]{"978-0-94045043-1"},
            new String[]{"<NUM>"});
  }

  public void testComplianceNumericFile() throws Exception {
    assertAnalyzesTo(
            a,
            "78academyawards/rules/rule02.html",
            new String[]{"78academyawards/rules/rule02.html"},
            new String[]{"<NUM>"});
  }

  public void testComplianceNumericWithUnderscores() throws Exception {
    assertAnalyzesTo(
            a,
            "2006-03-11t082958z_01_ban130523_rtridst_0_ozabs",
            new String[]{"2006-03-11t082958z_01_ban130523_rtridst_0_ozabs"},
            new String[]{"<NUM>"});
  }

  public void testComplianceNumericWithDash() throws Exception {
    assertAnalyzesTo(a, "mid-20th", new String[]{"mid-20th"},
            new String[]{"<NUM>"});
  }

  public void testComplianceManyTokens() throws Exception {
    assertAnalyzesTo(
            a,
            "/money.cnn.com/magazines/fortune/fortune_archive/2007/03/19/8402357/index.htm "
                    + "safari-0-sheikh-zayed-grand-mosque.jpg",
            new String[]{"money.cnn.com", "magazines", "fortune",
                    "fortune", "archive/2007/03/19/8402357", "index.htm",
                    "safari-0-sheikh", "zayed", "grand", "mosque.jpg"},
            new String[]{"<HOST>", "<ALPHANUM>", "<ALPHANUM>",
                    "<ALPHANUM>", "<NUM>", "<HOST>", "<NUM>", "<ALPHANUM>",
                    "<ALPHANUM>", "<HOST>"});
  }

  public void testJava14BWCompatibility() throws Exception {
    ClassicAnalyzer sa = new ClassicAnalyzer();
    assertAnalyzesTo(sa, "test\u02C6test", new String[] { "test", "test" });
    sa.close();
  }

  /**
   * Make sure we skip wicked long terms.
  */
  public void testWickedLongTerm() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    Analyzer analyzer = new ClassicAnalyzer();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));

    char[] chars = new char[IndexWriter.MAX_TERM_LENGTH];
    Arrays.fill(chars, 'x');
    Document doc = new Document();
    final String bigTerm = new String(chars);

    // This produces a too-long term:
    String contents = "abc xyz x" + bigTerm + " another term";
    doc.add(new TextField("content", contents, Field.Store.NO));
    writer.addDocument(doc);

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new TextField("content", "abc bbb ccc", Field.Store.NO));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);

    // Make sure all terms < max size were indexed
    assertEquals(2, reader.docFreq(new Term("content", "abc")));
    assertEquals(1, reader.docFreq(new Term("content", "bbb")));
    assertEquals(1, reader.docFreq(new Term("content", "term")));
    assertEquals(1, reader.docFreq(new Term("content", "another")));

    // Make sure position is still incremented when
    // massive term is skipped:
    PostingsEnum tps = MultiTerms.getTermPostingsEnum(reader,
                                                                "content",
                                                                new BytesRef("another"));
    assertTrue(tps.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, tps.freq());
    assertEquals(3, tps.nextPosition());

    // Make sure the doc that has the massive term is in
    // the index:
    assertEquals("document with wicked long term should is not in the index!", 2, reader.numDocs());

    reader.close();

    // Make sure we can add a document with exactly the
    // maximum length term, and search on that term:
    doc = new Document();
    doc.add(new TextField("content", bigTerm, Field.Store.NO));
    ClassicAnalyzer sa = new ClassicAnalyzer();
    sa.setMaxTokenLength(100000);
    writer  = new IndexWriter(dir, new IndexWriterConfig(sa));
    writer.addDocument(doc);
    writer.close();
    reader = DirectoryReader.open(dir);
    assertEquals(1, reader.docFreq(new Term("content", bigTerm)));
    reader.close();

    dir.close();
    analyzer.close();
    sa.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new ClassicAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Analyzer analyzer = new ClassicAnalyzer();
    checkRandomData(random(), analyzer, 10 * RANDOM_MULTIPLIER, 8192);
    analyzer.close();
  }
}
