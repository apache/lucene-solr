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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

/**
 * Tests {@link MultiSearcher} ranking, i.e. makes sure this bug is fixed:
 * http://issues.apache.org/bugzilla/show_bug.cgi?id=31841
 *
 * @version $Id: TestMultiSearcher.java 150492 2004-09-06 22:01:49Z dnaber $
 */
public class TestMultiSearcherRanking extends LuceneTestCase {
  
  private final boolean verbose = false;  // set to true to output hits
  private final String FIELD_NAME = "body";
  private Searcher multiSearcher;
  private Searcher singleSearcher;

  public void testOneTermQuery() throws IOException, ParseException {
    checkQuery("three");
  }

  public void testTwoTermQuery() throws IOException, ParseException {
    checkQuery("three foo");
  }

  public void testPrefixQuery() throws IOException, ParseException {
    checkQuery("multi*");
  }

  public void testFuzzyQuery() throws IOException, ParseException {
    checkQuery("multiThree~");
  }

  public void testRangeQuery() throws IOException, ParseException {
    checkQuery("{multiA TO multiP}");
  }

  public void testMultiPhraseQuery() throws IOException, ParseException {
      checkQuery("\"blueberry pi*\"");
  }

  public void testNoMatchQuery() throws IOException, ParseException {
    checkQuery("+three +nomatch");
  }

  /*
  public void testTermRepeatedQuery() throws IOException, ParseException {
    // TODO: this corner case yields different results.
    checkQuery("multi* multi* foo");
  }
  */

  /**
   * checks if a query yields the same result when executed on
   * a single IndexSearcher containing all documents and on a
   * MultiSearcher aggregating sub-searchers
   * @param queryStr  the query to check.
   * @throws IOException
   * @throws ParseException
   */
  private void checkQuery(String queryStr) throws IOException, ParseException {
    // check result hit ranking
    if(verbose) System.out.println("Query: " + queryStr);
      QueryParser queryParser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
    Query query = queryParser.parse(queryStr);
    ScoreDoc[] multiSearcherHits = multiSearcher.search(query, null, 1000).scoreDocs;
    ScoreDoc[] singleSearcherHits = singleSearcher.search(query, null, 1000).scoreDocs;
    assertEquals(multiSearcherHits.length, singleSearcherHits.length);
    for (int i = 0; i < multiSearcherHits.length; i++) {
      Document docMulti = multiSearcher.doc(multiSearcherHits[i].doc);
      Document docSingle = singleSearcher.doc(singleSearcherHits[i].doc);
      if(verbose) System.out.println("Multi:  " + docMulti.get(FIELD_NAME) + " score="
          + multiSearcherHits[i].score);
      if(verbose) System.out.println("Single: " + docSingle.get(FIELD_NAME) + " score="
          + singleSearcherHits[i].score);
      assertEquals(multiSearcherHits[i].score, singleSearcherHits[i].score,
          0.001f);
      assertEquals(docMulti.get(FIELD_NAME), docSingle.get(FIELD_NAME));
    }
    if(verbose) System.out.println();
  }
  
  /**
   * initializes multiSearcher and singleSearcher with the same document set
   */
  protected void setUp() throws Exception {
    super.setUp();
    // create MultiSearcher from two seperate searchers
    Directory d1 = new RAMDirectory();
    IndexWriter iw1 = new IndexWriter(d1, new StandardAnalyzer(), true,
                                      IndexWriter.MaxFieldLength.LIMITED);
    addCollection1(iw1);
    iw1.close();
    Directory d2 = new RAMDirectory();
    IndexWriter iw2 = new IndexWriter(d2, new StandardAnalyzer(), true,
                                      IndexWriter.MaxFieldLength.LIMITED);
    addCollection2(iw2);
    iw2.close();

    Searchable[] s = new Searchable[2];
    s[0] = new IndexSearcher(d1);
    s[1] = new IndexSearcher(d2);
    multiSearcher = new MultiSearcher(s);

    // create IndexSearcher which contains all documents
    Directory d = new RAMDirectory();
    IndexWriter iw = new IndexWriter(d, new StandardAnalyzer(), true,
                                     IndexWriter.MaxFieldLength.LIMITED);
    addCollection1(iw);
    addCollection2(iw);
    iw.close();
    singleSearcher = new IndexSearcher(d);
  }
  
  private void addCollection1(IndexWriter iw) throws IOException {
    add("one blah three", iw);
    add("one foo three multiOne", iw);
    add("one foobar three multiThree", iw);
    add("blueberry pie", iw);
    add("blueberry strudel", iw);
    add("blueberry pizza", iw);
  }

  private void addCollection2(IndexWriter iw) throws IOException {
    add("two blah three", iw);
    add("two foo xxx multiTwo", iw);
    add("two foobar xxx multiThreee", iw);
    add("blueberry chewing gum", iw);
    add("bluebird pizza", iw);
    add("bluebird foobar pizza", iw);
    add("piccadilly circus", iw);
  }
  
  private void add(String value, IndexWriter iw) throws IOException {
    Document d = new Document();
    d.add(new Field(FIELD_NAME, value, Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(d);
  }
  
}
