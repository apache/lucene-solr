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
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.store.Directory;


/**
 * Read index (abstract) task.
 * Sub classes implement withSearch(), withWarm(), withTraverse() and withRetrieve()
 * methods to configure the actual action.
 * <p/>
 * <p>Note: All ReadTasks reuse the reader if it is already open.
 * Otherwise a reader is opened at start and closed at the end.
 * <p/>
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
      IndexSearcher searcher = new IndexSearcher(ir);
      QueryMaker queryMaker = getQueryMaker();
      Query q = queryMaker.makeQuery();
      Sort sort = getSort();
      Hits hits;
      if(sort != null) {
        hits = searcher.search(q, sort);
      } else {
        hits = searcher.search(q);
      }
      //System.out.println("searched: "+q);

      if (withTraverse() && hits != null) {
        int traversalSize = Math.min(hits.length(), traversalSize());
        if (traversalSize > 0) {
          boolean retrieve = withRetrieve();
          int numHighlight = Math.min(numToHighlight(), hits.length());
          Analyzer analyzer = getRunData().getAnalyzer();
          Highlighter highlighter = null;
          int maxFrags = 1;
          if (numHighlight > 0) {
            highlighter = getHighlighter(q);
            maxFrags = maxNumFragments();
          }
          boolean merge = isMergeContiguousFragments();
          for (int m = 0; m < traversalSize; m++) {
            int id = hits.id(m);
            res++;
            if (retrieve) {
              Document document = retrieveDoc(ir, id);
              res += document != null ? 1 : 0;
              if (numHighlight > 0 && m < numHighlight) {
                Collection/*<String>*/ fieldsToHighlight = getFieldsToHighlight(document);
                for (Iterator iterator = fieldsToHighlight.iterator(); iterator.hasNext();) {
                  String field = (String) iterator.next();
                  String text = document.get(field);
                  TokenStream ts = TokenSources.getAnyTokenStream(ir, id, field, document, analyzer);
                  res += doHighlight(ts, text, highlighter, merge, maxFrags);
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

  protected Highlighter getHighlighter(Query q){
    return new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(q));
  }

  /**
   *
   * @return the maxiumum number of highlighter fragments
   */
  public int maxNumFragments(){
    return 10;
  }

  /**
   *
   * @return true if the highlighter should merge contiguous fragments
   */
  public boolean isMergeContiguousFragments(){
    return false;
  }

  protected int doHighlight(TokenStream ts, String text,  Highlighter highlighter, boolean mergeContiguous, int maxFragments) throws IOException {
    TextFragment[] frag = highlighter.getBestTextFragments(ts, text, mergeContiguous, maxFragments);
    return frag != null ? frag.length : 0;
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
