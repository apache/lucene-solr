package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/** Implements search over a set of <code>Searchables</code>.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query)}
 * or {@link #search(Query,Filter)} methods.
 */
public class MultiSearcher extends Searcher {
  private Searchable[] searchables;
  private int[] starts;
  private int maxDoc = 0;

  /** Creates a searcher which searches <i>searchables</i>. */
  public MultiSearcher(Searchable[] searchables) throws IOException {
    this.searchables = searchables;

    starts = new int[searchables.length + 1];	  // build starts array
    for (int i = 0; i < searchables.length; i++) {
      starts[i] = maxDoc;
      maxDoc += searchables[i].maxDoc();          // compute maxDocs
    }
    starts[searchables.length] = maxDoc;
  }
  
  /** Return the array of {@link Searchable}s this searches. */
  public Searchable[] getSearchables() {
    return searchables;
  }

  protected int[] getStarts() {
  	return starts;
  }

  // inherit javadoc
  public void close() throws IOException {
    for (int i = 0; i < searchables.length; i++)
      searchables[i].close();
  }

  public int docFreq(Term term) throws IOException {
    int docFreq = 0;
    for (int i = 0; i < searchables.length; i++)
      docFreq += searchables[i].docFreq(term);
    return docFreq;
  }

  // inherit javadoc
  public Document doc(int n) throws IOException {
    int i = subSearcher(n);			  // find searcher index
    return searchables[i].doc(n - starts[i]);	  // dispatch to searcher
  }

  /** Call {@link #subSearcher} instead.
   * @deprecated
   */
  public int searcherIndex(int n) {
    return subSearcher(n);
  }

  /** Returns index of the searcher for document <code>n</code> in the array
   * used to construct this searcher. */
  public int subSearcher(int n) {                 // find searcher for doc n:
    // replace w/ call to Arrays.binarySearch in Java 1.2
    int lo = 0;					  // search starts array
    int hi = searchables.length - 1;		  // for first element less
						  // than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >> 1;
      int midValue = starts[mid];
      if (n < midValue)
	hi = mid - 1;
      else if (n > midValue)
	lo = mid + 1;
      else {                                      // found a match
        while (mid+1 < searchables.length && starts[mid+1] == midValue) {
          mid++;                                  // scan to last match
        }
	return mid;
      }
    }
    return hi;
  }

  /** Returns the document number of document <code>n</code> within its
   * sub-index. */
  public int subDoc(int n) {
    return n - starts[subSearcher(n)];
  }

  public int maxDoc() throws IOException {
    return maxDoc;
  }

  public TopDocs search(Query query, Filter filter, int nDocs)
      throws IOException {
    HitQueue hq = new HitQueue(nDocs);
    int totalHits = 0;

    for (int i = 0; i < searchables.length; i++) { // search each searcher
      TopDocs docs = searchables[i].search(query, filter, nDocs);
      totalHits += docs.totalHits;		  // update totalHits
      ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
	ScoreDoc scoreDoc = scoreDocs[j];
        scoreDoc.doc += starts[i];                // convert doc
        if(!hq.insert(scoreDoc))
            break;                                // no more scores > minScore
      }
    }

    ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size()-1; i >= 0; i--)	  // put docs in array
      scoreDocs[i] = (ScoreDoc)hq.pop();

    return new TopDocs(totalHits, scoreDocs);
  }


  public TopFieldDocs search (Query query, Filter filter, int n, Sort sort)
    throws IOException {
    FieldDocSortedHitQueue hq = null;
    int totalHits = 0;

    for (int i = 0; i < searchables.length; i++) { // search each searcher
      TopFieldDocs docs = searchables[i].search (query, filter, n, sort);
      if (hq == null) hq = new FieldDocSortedHitQueue (docs.fields, n);
      totalHits += docs.totalHits;		  // update totalHits
      ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
        ScoreDoc scoreDoc = scoreDocs[j];
        scoreDoc.doc += starts[i];                // convert doc
        if (!hq.insert (scoreDoc))
          break;                                  // no more scores > minScore
      }
    }

    ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size() - 1; i >= 0; i--)	  // put docs in array
      scoreDocs[i] = (ScoreDoc) hq.pop();

    return new TopFieldDocs (totalHits, scoreDocs, hq.getFields());
  }


  // inherit javadoc
  public void search(Query query, Filter filter, final HitCollector results)
    throws IOException {
    for (int i = 0; i < searchables.length; i++) {

      final int start = starts[i];

      searchables[i].search(query, filter, new HitCollector() {
	  public void collect(int doc, float score) {
	    results.collect(doc + start, score);
	  }
	});

    }
  }

  public Query rewrite(Query original) throws IOException {
    Query[] queries = new Query[searchables.length];
    for (int i = 0; i < searchables.length; i++) {
      queries[i] = searchables[i].rewrite(original);
    }
    return original.combine(queries);
  }

  public Explanation explain(Query query, int doc) throws IOException {
    int i = subSearcher(doc);			  // find searcher index
    return searchables[i].explain(query,doc-starts[i]); // dispatch to searcher
  }

}
