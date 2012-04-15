package org.apache.lucene.search;

/**
 * Copyright 2005 The Apache Software Foundation
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
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Unit test for sorting code. */
public class TestCustomSearcherSort extends LuceneTestCase {
  
  private Directory index = null;
  private IndexReader reader;
  private Query query = null;
  // reduced from 20000 to 2000 to speed up test...
  private int INDEX_SIZE;
  
  /**
   * Create index and query for test cases.
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    INDEX_SIZE = atLeast(2000);
    index = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), index);
    RandomGen random = new RandomGen(random());
    for (int i = 0; i < INDEX_SIZE; ++i) { // don't decrease; if to low the
                                           // problem doesn't show up
      Document doc = new Document();
      if ((i % 5) != 0) { // some documents must not have an entry in the first
                          // sort field
        doc.add(newField("publicationDate_", random.getLuceneDate(), StringField.TYPE_STORED));
      }
      if ((i % 7) == 0) { // some documents to match the query (see below)
        doc.add(newField("content", "test", TextField.TYPE_STORED));
      }
      // every document has a defined 'mandant' field
      doc.add(newField("mandant", Integer.toString(i % 3), StringField.TYPE_STORED));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    query = new TermQuery(new Term("content", "test"));
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    index.close();
    super.tearDown();
  }
  
  /**
   * Run the test using two CustomSearcher instances.
   */
  public void testFieldSortCustomSearcher() throws Exception {
    // log("Run testFieldSortCustomSearcher");
    // define the sort criteria
    Sort custSort = new Sort(
        new SortField("publicationDate_", SortField.Type.STRING),
        SortField.FIELD_SCORE);
    IndexSearcher searcher = new CustomSearcher(reader, 2);
    // search and check hits
    matchHits(searcher, custSort);
  }
  
  /**
   * Run the test using one CustomSearcher wrapped by a MultiSearcher.
   */
  public void testFieldSortSingleSearcher() throws Exception {
    // log("Run testFieldSortSingleSearcher");
    // define the sort criteria
    Sort custSort = new Sort(
        new SortField("publicationDate_", SortField.Type.STRING),
        SortField.FIELD_SCORE);
    IndexSearcher searcher = new CustomSearcher(reader, 2);
    // search and check hits
    matchHits(searcher, custSort);
  }
  
  // make sure the documents returned by the search match the expected list
  private void matchHits(IndexSearcher searcher, Sort sort) throws IOException {
    // make a query without sorting first
    ScoreDoc[] hitsByRank = searcher.search(query, null, Integer.MAX_VALUE).scoreDocs;
    checkHits(hitsByRank, "Sort by rank: "); // check for duplicates
    Map<Integer,Integer> resultMap = new TreeMap<Integer,Integer>();
    // store hits in TreeMap - TreeMap does not allow duplicates; existing
    // entries are silently overwritten
    for (int hitid = 0; hitid < hitsByRank.length; ++hitid) {
      resultMap.put(Integer.valueOf(hitsByRank[hitid].doc), // Key: Lucene
                                                            // Document ID
          Integer.valueOf(hitid)); // Value: Hits-Objekt Index
    }
    
    // now make a query using the sort criteria
    ScoreDoc[] resultSort = searcher.search(query, null, Integer.MAX_VALUE,
        sort).scoreDocs;
    checkHits(resultSort, "Sort by custom criteria: "); // check for duplicates
    
    // besides the sorting both sets of hits must be identical
    for (int hitid = 0; hitid < resultSort.length; ++hitid) {
      Integer idHitDate = Integer.valueOf(resultSort[hitid].doc); // document ID
                                                                  // from sorted
                                                                  // search
      if (!resultMap.containsKey(idHitDate)) {
        log("ID " + idHitDate + " not found. Possibliy a duplicate.");
      }
      assertTrue(resultMap.containsKey(idHitDate)); // same ID must be in the
                                                    // Map from the rank-sorted
                                                    // search
      // every hit must appear once in both result sets --> remove it from the
      // Map.
      // At the end the Map must be empty!
      resultMap.remove(idHitDate);
    }
    if (resultMap.size() == 0) {
      // log("All hits matched");
    } else {
      log("Couldn't match " + resultMap.size() + " hits.");
    }
    assertEquals(resultMap.size(), 0);
  }
  
  /**
   * Check the hits for duplicates.
   * 
   * @param hits
   */
  private void checkHits(ScoreDoc[] hits, String prefix) {
    if (hits != null) {
      Map<Integer,Integer> idMap = new TreeMap<Integer,Integer>();
      for (int docnum = 0; docnum < hits.length; ++docnum) {
        Integer luceneId = null;
        
        luceneId = Integer.valueOf(hits[docnum].doc);
        if (idMap.containsKey(luceneId)) {
          StringBuilder message = new StringBuilder(prefix);
          message.append("Duplicate key for hit index = ");
          message.append(docnum);
          message.append(", previous index = ");
          message.append((idMap.get(luceneId)).toString());
          message.append(", Lucene ID = ");
          message.append(luceneId);
          log(message.toString());
        } else {
          idMap.put(luceneId, Integer.valueOf(docnum));
        }
      }
    }
  }
  
  // Simply write to console - choosen to be independant of log4j etc
  private void log(String message) {
    if (VERBOSE) System.out.println(message);
  }
  
  public class CustomSearcher extends IndexSearcher {
    private int switcher;
    
    /**
     * @param r
     */
    public CustomSearcher(IndexReader r, int switcher) {
      super(r);
      this.switcher = switcher;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.Searchable#search(org.apache.lucene.search.Query
     * , org.apache.lucene.search.Filter, int, org.apache.lucene.search.Sort)
     */
    @Override
    public TopFieldDocs search(Query query, Filter filter, int nDocs, Sort sort)
        throws IOException {
      BooleanQuery bq = new BooleanQuery();
      bq.add(query, BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("mandant", Integer.toString(switcher))),
          BooleanClause.Occur.MUST);
      return super.search(bq, filter, nDocs, sort);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lucene.search.Searchable#search(org.apache.lucene.search.Query
     * , org.apache.lucene.search.Filter, int)
     */
    @Override
    public TopDocs search(Query query, Filter filter, int nDocs)
        throws IOException {
      BooleanQuery bq = new BooleanQuery();
      bq.add(query, BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("mandant", Integer.toString(switcher))),
          BooleanClause.Occur.MUST);
      return super.search(bq, filter, nDocs);
    }
  }
  
  private class RandomGen {
    RandomGen(Random random) {
      this.random = random;
    }
    
    private Random random;
    private Calendar base = new GregorianCalendar(1980, 1, 1);
    
    // Just to generate some different Lucene Date strings
    private String getLuceneDate() {
      return DateTools.timeToString(base.getTimeInMillis() + random.nextInt()
          - Integer.MIN_VALUE, DateTools.Resolution.DAY);
    }
  }
}
