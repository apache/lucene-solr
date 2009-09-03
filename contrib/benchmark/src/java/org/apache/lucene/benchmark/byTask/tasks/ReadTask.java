package org.apache.lucene.benchmark.byTask.tasks;

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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.store.Directory;


/**
 * Read index (abstract) task.
 * Sub classes implement withSearch(), withWarm(), withTraverse() and withRetrieve()
 * methods to configure the actual action.
 * <p/>
 * <p>Note: All ReadTasks reuse the reader if it is already open.
 * Otherwise a reader is opened at start and closed at the end.
 * <p>
 * The <code>search.num.hits</code> config parameter sets
 * the top number of hits to collect during searching.
 * <p>Other side effects: none.
 */
public abstract class ReadTask extends PerfTask {

  public ReadTask(PerfRunData runData) {
    super(runData);
  }
  public int doLogic() throws Exception {
    int res = 0;
    boolean closeReader = false;

    // open reader or use existing one
    IndexReader ir = getRunData().getIndexReader();
    if (ir == null) {
      Directory dir = getRunData().getDirectory();
      ir = IndexReader.open(dir);
      closeReader = true;
      //res++; //this is confusing, comment it out
    }

    // optionally warm and add num docs traversed to count
    if (withWarm()) {
      Document doc = null;
      for (int m = 0; m < ir.maxDoc(); m++) {
        if (!ir.isDeleted(m)) {
          doc = ir.document(m);
          res += (doc == null ? 0 : 1);
        }
      }
    }

    if (withSearch()) {
      res++;
      final IndexSearcher searcher;
      if (closeReader) {
        searcher = new IndexSearcher(ir);
      } else {
        searcher = getRunData().getIndexSearcher();
      }
      QueryMaker queryMaker = getQueryMaker();
      Query q = queryMaker.makeQuery();
      Sort sort = getSort();
      TopDocs hits;
      final int numHits = numHits();
      if (numHits > 0) {
        if (sort != null) {
          // TODO: change the following to create TFC with in/out-of order
          // according to whether the query's Scorer.
          TopFieldCollector collector = TopFieldCollector.create(sort, numHits,
              true, withScore(), withMaxScore(), false);
          searcher.search(q, collector);
          hits = collector.topDocs();
        } else {
          hits = searcher.search(q, numHits);
        }
        //System.out.println("q=" + q + ":" + hits.totalHits + " total hits"); 

        if (withTraverse()) {
          final ScoreDoc[] scoreDocs = hits.scoreDocs;
          int traversalSize = Math.min(scoreDocs.length, traversalSize());

          if (traversalSize > 0) {
            boolean retrieve = withRetrieve();
            int numHighlight = Math.min(numToHighlight(), scoreDocs.length);
            Analyzer analyzer = getRunData().getAnalyzer();
            BenchmarkHighlighter highlighter = null;
            if (numHighlight > 0) {
              highlighter = getBenchmarkHighlighter(q);
            }
            for (int m = 0; m < traversalSize; m++) {
              int id = scoreDocs[m].doc;
              res++;
              if (retrieve) {
                Document document = retrieveDoc(ir, id);
                res += document != null ? 1 : 0;
                if (numHighlight > 0 && m < numHighlight) {
                  Collection/*<String>*/ fieldsToHighlight = getFieldsToHighlight(document);
                  for (Iterator iterator = fieldsToHighlight.iterator(); iterator.hasNext();) {
                    String field = (String) iterator.next();
                    String text = document.get(field);
                    res += highlighter.doHighlight(ir, id, field, document, analyzer, text);
                  }
                }
              }
            }
          }
        }
      }

      searcher.close();
    }

    if (closeReader) {
      ir.close();
    }
    return res;
  }



  protected Document retrieveDoc(IndexReader ir, int id) throws IOException {
    return ir.document(id);
  }

  /**
   * Return query maker used for this task.
   */
  public abstract QueryMaker getQueryMaker();

  /**
   * Return true if search should be performed.
   */
  public abstract boolean withSearch();
  

  /**
   * Return true if warming should be performed.
   */
  public abstract boolean withWarm();

  /**
   * Return true if, with search, results should be traversed.
   */
  public abstract boolean withTraverse();

  /** Whether scores should be computed (only useful with
   *  field sort) */
  public boolean withScore() {
    return true;
  }

  /** Whether maxScores should be computed (only useful with
   *  field sort) */
  public boolean withMaxScore() {
    return true;
  }

  /**
   * Specify the number of hits to traverse.  Tasks should override this if they want to restrict the number
   * of hits that are traversed when {@link #withTraverse()} is true. Must be greater than 0.
   * <p/>
   * Read task calculates the traversal as: Math.min(hits.length(), traversalSize())
   *
   * @return Integer.MAX_VALUE
   */
  public int traversalSize() {
    return Integer.MAX_VALUE;
  }

  static final int DEFAULT_SEARCH_NUM_HITS = 10;
  private int numHits;

  public void setup() throws Exception {
    super.setup();
    numHits = getRunData().getConfig().get("search.num.hits", DEFAULT_SEARCH_NUM_HITS);
  }

  /**
   * Specify the number of hits to retrieve.  Tasks should override this if they want to restrict the number
   * of hits that are collected during searching. Must be greater than 0.
   *
   * @return 10 by default, or search.num.hits config if set.
   */
  public int numHits() {
    return numHits;
  }

  /**
   * Return true if, with search & results traversing, docs should be retrieved.
   */
  public abstract boolean withRetrieve();

  /**
   * Set to the number of documents to highlight.
   *
   * @return The number of the results to highlight.  O means no docs will be highlighted.
   */
  public int numToHighlight() {
    return 0;
  }

  /**
   * @deprecated Use {@link #getBenchmarkHighlighter(Query)}
   */
  final Highlighter getHighlighter(Query q) {
    // not called
    return null;
  }
  
  /**
   * Return an appropriate highlighter to be used with
   * highlighting tasks
   */
  protected BenchmarkHighlighter getBenchmarkHighlighter(Query q){
    return null;
  }

  /**
   * @return the maximum number of highlighter fragments
   * @deprecated Please define getBenchmarkHighlighter instead
   */
  final int maxNumFragments(){
    // not called -- we switched this method to final to
    // force any external subclasses to cutover to
    // getBenchmarkHighlighter instead
    return 10;
  }

  /**
   *
   * @return true if the highlighter should merge contiguous fragments
   * @deprecated Please define getBenchmarkHighlighter instead
   */
  final boolean isMergeContiguousFragments(){
    // not called -- we switched this method to final to
    // force any external subclasses to cutover to
    // getBenchmarkHighlighter instead
    return false;
  }

  /**
   * @deprecated Please define getBenchmarkHighlighter instead
   */
  final int doHighlight(TokenStream ts, String text,  Highlighter highlighter, boolean mergeContiguous, int maxFragments) throws IOException, InvalidTokenOffsetsException {
    // not called -- we switched this method to final to
    // force any external subclasses to cutover to
    // getBenchmarkHighlighter instead
    return 0;
  }
  
  protected Sort getSort() {
    return null;
  }

  /**
   * Define the fields to highlight.  Base implementation returns all fields
   * @param document The Document
   * @return A Collection of Field names (Strings)
   */
  protected Collection/*<String>*/ getFieldsToHighlight(Document document) {
    List/*<Fieldable>*/ fieldables = document.getFields();
    Set/*<String>*/ result = new HashSet(fieldables.size());
    for (Iterator iterator = fieldables.iterator(); iterator.hasNext();) {
      Fieldable fieldable = (Fieldable) iterator.next();
      result.add(fieldable.name());
    }
    return result;
  }

}
