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

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.PriorityQueue;

/** Implements parallel search over a set of <code>Searchables</code>.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query)}
 * or {@link #search(Query,Filter)} methods.
 */
public class ParallelMultiSearcher extends MultiSearcher {

  private Searchable[] searchables;
  private int[] starts;
	
  /** Creates a searcher which searches <i>searchables</i>. */
  public ParallelMultiSearcher(Searchable[] searchables) throws IOException {
    super(searchables);
    this.searchables=searchables;
    this.starts=getStarts();
  }

  /**
   * TODO: parallelize this one too
   */
  public int docFreq(Term term) throws IOException {
    return super.docFreq(term);
  }

  /**
   * A search implementation which spans a new thread for each
   * Searchable, waits for each search to complete and merge
   * the results back together.
   */
  public TopDocs search(Weight weight, Filter filter, int nDocs)
    throws IOException {
    HitQueue hq = new HitQueue(nDocs);
    int totalHits = 0;
    MultiSearcherThread[] msta =
      new MultiSearcherThread[searchables.length];
    for (int i = 0; i < searchables.length; i++) { // search each searcher
      // Assume not too many searchables and cost of creating a thread is by far inferior to a search
      msta[i] =
        new MultiSearcherThread(
                                searchables[i],
                                weight,
                                filter,
                                nDocs,
                                hq,
                                i,
                                starts,
                                "MultiSearcher thread #" + (i + 1));
      msta[i].start();
    }

    for (int i = 0; i < searchables.length; i++) {
      try {
        msta[i].join();
      } catch (InterruptedException ie) {
        ; // TODO: what should we do with this???
      }
      IOException ioe = msta[i].getIOException();
      if (ioe == null) {
        totalHits += msta[i].hits();
      } else {
        // if one search produced an IOException, rethrow it
        throw ioe;
      }
    }

    ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
      scoreDocs[i] = (ScoreDoc) hq.pop();

    float maxScore = (totalHits==0) ? Float.NEGATIVE_INFINITY : scoreDocs[0].score;
    
    return new TopDocs(totalHits, scoreDocs, maxScore);
  }

  /**
   * A search implementation allowing sorting which spans a new thread for each
   * Searchable, waits for each search to complete and merges
   * the results back together.
   */
  public TopFieldDocs search(Weight weight, Filter filter, int nDocs, Sort sort)
    throws IOException {
    // don't specify the fields - we'll wait to do this until we get results
    FieldDocSortedHitQueue hq = new FieldDocSortedHitQueue (null, nDocs);
    int totalHits = 0;
    MultiSearcherThread[] msta = new MultiSearcherThread[searchables.length];
    for (int i = 0; i < searchables.length; i++) { // search each searcher
      // Assume not too many searchables and cost of creating a thread is by far inferior to a search
      msta[i] =
        new MultiSearcherThread(
                                searchables[i],
                                weight,
                                filter,
                                nDocs,
                                hq,
                                sort,
                                i,
                                starts,
                                "MultiSearcher thread #" + (i + 1));
      msta[i].start();
    }

    float maxScore=Float.NEGATIVE_INFINITY;
    
    for (int i = 0; i < searchables.length; i++) {
      try {
        msta[i].join();
      } catch (InterruptedException ie) {
        ; // TODO: what should we do with this???
      }
      IOException ioe = msta[i].getIOException();
      if (ioe == null) {
        totalHits += msta[i].hits();
        maxScore=Math.max(maxScore, msta[i].getMaxScore());
      } else {
        // if one search produced an IOException, rethrow it
        throw ioe;
      }
    }

    ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
      scoreDocs[i] = (ScoreDoc) hq.pop();

    return new TopFieldDocs(totalHits, scoreDocs, hq.getFields(), maxScore);
  }

  /** Lower-level search API.
   *
   * <p>{@link HitCollector#collect(int,float)} is called for every matching
   * document.
   *
   * <p>Applications should only use this if they need <i>all</i> of the
   * matching documents.  The high-level search API ({@link
   * Searcher#search(Query)}) is usually more efficient, as it skips
   * non-high-scoring hits.
   *
   * @param weight to match documents
   * @param filter if non-null, a bitset used to eliminate some documents
   * @param results to receive hits
   * 
   * @todo parallelize this one too
   */
  public void search(Weight weight, Filter filter, final HitCollector results)
    throws IOException {
    for (int i = 0; i < searchables.length; i++) {

      final int start = starts[i];

      searchables[i].search(weight, filter, new HitCollector() {
          public void collect(int doc, float score) {
            results.collect(doc + start, score);
          }
        });

    }
  }

  /*
   * TODO: this one could be parallelized too
   * @see org.apache.lucene.search.Searchable#rewrite(org.apache.lucene.search.Query)
   */
  public Query rewrite(Query original) throws IOException {
    return super.rewrite(original);
  }

}

/**
 * A thread subclass for searching a single searchable 
 */
class MultiSearcherThread extends Thread {

  private Searchable searchable;
  private Weight weight;
  private Filter filter;
  private int nDocs;
  private TopDocs docs;
  private int i;
  private PriorityQueue hq;
  private int[] starts;
  private IOException ioe;
  private Sort sort;

  public MultiSearcherThread(
                             Searchable searchable,
                             Weight weight,
                             Filter filter,
                             int nDocs,
                             HitQueue hq,
                             int i,
                             int[] starts,
                             String name) {
    super(name);
    this.searchable = searchable;
    this.weight = weight;
    this.filter = filter;
    this.nDocs = nDocs;
    this.hq = hq;
    this.i = i;
    this.starts = starts;
  }

  public MultiSearcherThread(
                             Searchable searchable,
                             Weight weight,
                             Filter filter,
                             int nDocs,
                             FieldDocSortedHitQueue hq,
                             Sort sort,
                             int i,
                             int[] starts,
                             String name) {
    super(name);
    this.searchable = searchable;
    this.weight = weight;
    this.filter = filter;
    this.nDocs = nDocs;
    this.hq = hq;
    this.i = i;
    this.starts = starts;
    this.sort = sort;
  }

  public void run() {
    try {
      docs = (sort == null) ? searchable.search (weight, filter, nDocs)
        : searchable.search (weight, filter, nDocs, sort);
    }
    // Store the IOException for later use by the caller of this thread
    catch (IOException ioe) {
      this.ioe = ioe;
    }
    if (ioe == null) {
      // if we are sorting by fields, we need to tell the field sorted hit queue
      // the actual type of fields, in case the original list contained AUTO.
      // if the searchable returns null for fields, we'll have problems.
      if (sort != null) {
        ((FieldDocSortedHitQueue)hq).setFields (((TopFieldDocs)docs).fields);
      }
      ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0;
           j < scoreDocs.length;
           j++) { // merge scoreDocs into hq
        ScoreDoc scoreDoc = scoreDocs[j];
        scoreDoc.doc += starts[i]; // convert doc 
        //it would be so nice if we had a thread-safe insert 
        synchronized (hq) {
          if (!hq.insert(scoreDoc))
            break;
        } // no more scores > minScore
      }
    }
  }

  public int hits() {
    return docs.totalHits;
  }

  public float getMaxScore() {
      return docs.getMaxScore();
  }
  
  public IOException getIOException() {
    return ioe;
  }

}
