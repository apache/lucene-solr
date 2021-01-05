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
package org.apache.lucene.benchmark.byTask.tasks;

import java.io.IOException;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;

/**
 * Read index (abstract) task. Sub classes implement withSearch(), withWarm(), withTraverse() and
 * withRetrieve() methods to configure the actual action.
 *
 * <p>Note: All ReadTasks reuse the reader if it is already open. Otherwise a reader is opened at
 * start and closed at the end.
 *
 * <p>The <code>search.num.hits</code> config parameter sets the top number of hits to collect
 * during searching. If <code>print.hits.field</code> is set, then each hit is printed along with
 * the value of that field.
 *
 * <p>Other side effects: none.
 */
public abstract class ReadTask extends PerfTask {

  private final QueryMaker queryMaker;

  public ReadTask(PerfRunData runData) {
    super(runData);
    if (withSearch()) {
      queryMaker = getQueryMaker();
    } else {
      queryMaker = null;
    }
  }

  @Override
  public int doLogic() throws Exception {
    int res = 0;

    // open reader or use existing one
    IndexSearcher searcher = getRunData().getIndexSearcher(); // (will incRef the reader)

    IndexReader reader;

    final boolean closeSearcher;
    if (searcher == null) {
      // open our own reader
      Directory dir = getRunData().getDirectory();
      reader = DirectoryReader.open(dir);
      searcher = new IndexSearcher(reader);
      closeSearcher = true;
    } else {
      // use existing one; this passes +1 ref to us
      reader = searcher.getIndexReader();
      closeSearcher = false;
    }

    // optionally warm and add num docs traversed to count
    if (withWarm()) {
      Document doc = null;
      Bits liveDocs = MultiBits.getLiveDocs(reader);
      for (int m = 0; m < reader.maxDoc(); m++) {
        if (null == liveDocs || liveDocs.get(m)) {
          doc = reader.document(m);
          res += (doc == null ? 0 : 1);
        }
      }
    }

    if (withSearch()) {
      res++;
      Query q = queryMaker.makeQuery();
      Sort sort = getSort();
      TopDocs hits = null;
      final int numHits = numHits();
      if (numHits > 0) {
        if (withCollector() == false) {
          if (sort != null) {
            // TODO: instead of always passing false we
            // should detect based on the query; if we make
            // the IndexSearcher search methods that take
            // Weight public again, we can go back to
            // pulling the Weight ourselves:
            TopFieldCollector collector =
                TopFieldCollector.create(sort, numHits, withTotalHits() ? Integer.MAX_VALUE : 1);
            searcher.search(q, collector);
            hits = collector.topDocs();
          } else {
            hits = searcher.search(q, numHits);
          }
        } else {
          Collector collector = createCollector();
          searcher.search(q, collector);
          // hits = collector.topDocs();
        }

        if (hits != null) {
          final String printHitsField = getRunData().getConfig().get("print.hits.field", null);
          if (printHitsField != null && printHitsField.length() > 0) {
            System.out.println("totalHits = " + hits.totalHits);
            System.out.println("maxDoc()  = " + reader.maxDoc());
            System.out.println("numDocs() = " + reader.numDocs());
            for (int i = 0; i < hits.scoreDocs.length; i++) {
              final int docID = hits.scoreDocs[i].doc;
              final Document doc = reader.document(docID);
              System.out.println(
                  "  "
                      + i
                      + ": doc="
                      + docID
                      + " score="
                      + hits.scoreDocs[i].score
                      + " "
                      + printHitsField
                      + " ="
                      + doc.get(printHitsField));
            }
          }

          res += withTopDocs(searcher, q, hits);
        }
      }
    }

    if (closeSearcher) {
      reader.close();
    } else {
      // Release our +1 ref from above
      reader.decRef();
    }
    return res;
  }

  protected int withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
    IndexReader reader = searcher.getIndexReader();
    int res = 0;
    if (withTraverse()) {
      final ScoreDoc[] scoreDocs = hits.scoreDocs;
      int traversalSize = Math.min(scoreDocs.length, traversalSize());

      if (traversalSize > 0) {
        boolean retrieve = withRetrieve();
        for (int m = 0; m < traversalSize; m++) {
          int id = scoreDocs[m].doc;
          res++;
          if (retrieve) {
            Document document = retrieveDoc(reader, id);
            res += document != null ? 1 : 0;
          }
        }
      }
    }
    return res;
  }

  protected Collector createCollector() throws Exception {
    return TopScoreDocCollector.create(numHits(), withTotalHits() ? Integer.MAX_VALUE : 1);
  }

  protected Document retrieveDoc(IndexReader ir, int id) throws IOException {
    return ir.document(id);
  }

  /** Return query maker used for this task. */
  public abstract QueryMaker getQueryMaker();

  /** Return true if search should be performed. */
  public abstract boolean withSearch();

  public boolean withCollector() {
    return false;
  }

  /** Return true if warming should be performed. */
  public abstract boolean withWarm();

  /** Return true if, with search, results should be traversed. */
  public abstract boolean withTraverse();

  /** Whether totalHits should be computed (only useful with field sort) */
  public boolean withTotalHits() {
    return false;
  }

  /**
   * Specify the number of hits to traverse. Tasks should override this if they want to restrict the
   * number of hits that are traversed when {@link #withTraverse()} is true. Must be greater than 0.
   *
   * <p>Read task calculates the traversal as: Math.min(hits.length(), traversalSize())
   *
   * @return Integer.MAX_VALUE
   */
  public int traversalSize() {
    return Integer.MAX_VALUE;
  }

  static final int DEFAULT_SEARCH_NUM_HITS = 10;
  private int numHits;

  @Override
  public void setup() throws Exception {
    super.setup();
    numHits = getRunData().getConfig().get("search.num.hits", DEFAULT_SEARCH_NUM_HITS);
  }

  /**
   * Specify the number of hits to retrieve. Tasks should override this if they want to restrict the
   * number of hits that are collected during searching. Must be greater than 0.
   *
   * @return 10 by default, or search.num.hits config if set.
   */
  public int numHits() {
    return numHits;
  }

  /** Return true if, with search and results traversing, docs should be retrieved. */
  public abstract boolean withRetrieve();

  protected Sort getSort() {
    return null;
  }
}
