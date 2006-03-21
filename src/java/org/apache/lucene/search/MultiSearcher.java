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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/** Implements search over a set of <code>Searchables</code>.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query)}
 * or {@link #search(Query,Filter)} methods.
 */
public class MultiSearcher extends Searcher {
    /**
     * Document Frequency cache acting as a Dummy-Searcher.
     * This class is no full-fledged Searcher, but only supports
     * the methods necessary to initialize Weights.
     */
  private static class CachedDfSource extends Searcher {
    private Map dfMap; // Map from Terms to corresponding doc freqs
    private int maxDoc; // document count

    public CachedDfSource(Map dfMap, int maxDoc) {
      this.dfMap = dfMap;
      this.maxDoc = maxDoc;
    }

    public int docFreq(Term term) {
      int df;
      try {
        df = ((Integer) dfMap.get(term)).intValue();
      } catch (NullPointerException e) {
        throw new IllegalArgumentException("df for term " + term.text()
            + " not available");
      }
      return df;
    }

    public int[] docFreqs(Term[] terms) {
      int[] result = new int[terms.length];
      for (int i = 0; i < terms.length; i++) {
        result[i] = docFreq(terms[i]);
      }
      return result;
    }

    public int maxDoc() {
      return maxDoc;
    }

    public Query rewrite(Query query) {
      // this is a bit of a hack. We know that a query which
      // creates a Weight based on this Dummy-Searcher is
      // always already rewritten (see preparedWeight()).
      // Therefore we just return the unmodified query here
      return query;
    }

    public void close() {
      throw new UnsupportedOperationException();
    }

    public Document doc(int i) {
      throw new UnsupportedOperationException();
    }

    public Explanation explain(Weight weight,int doc) {
      throw new UnsupportedOperationException();
    }

    public void search(Weight weight, Filter filter, HitCollector results) {
      throw new UnsupportedOperationException();
    }

    public TopDocs search(Weight weight,Filter filter,int n) {
      throw new UnsupportedOperationException();
    }

    public TopFieldDocs search(Weight weight,Filter filter,int n,Sort sort) {
      throw new UnsupportedOperationException();
    }
  };


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

  public TopDocs search(Weight weight, Filter filter, int nDocs)
  throws IOException {

    HitQueue hq = new HitQueue(nDocs);
    int totalHits = 0;

    for (int i = 0; i < searchables.length; i++) { // search each searcher
      TopDocs docs = searchables[i].search(weight, filter, nDocs);
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
    
    float maxScore = (totalHits==0) ? Float.NEGATIVE_INFINITY : scoreDocs[0].score;
    
    return new TopDocs(totalHits, scoreDocs, maxScore);
  }

  public TopFieldDocs search (Weight weight, Filter filter, int n, Sort sort)
  throws IOException {
    FieldDocSortedHitQueue hq = null;
    int totalHits = 0;

    float maxScore=Float.NEGATIVE_INFINITY;
    
    for (int i = 0; i < searchables.length; i++) { // search each searcher
      TopFieldDocs docs = searchables[i].search (weight, filter, n, sort);
      
      if (hq == null) hq = new FieldDocSortedHitQueue (docs.fields, n);
      totalHits += docs.totalHits;		  // update totalHits
      maxScore = Math.max(maxScore, docs.getMaxScore());
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

    return new TopFieldDocs (totalHits, scoreDocs, hq.getFields(), maxScore);
  }


  // inherit javadoc
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

  public Query rewrite(Query original) throws IOException {
    Query[] queries = new Query[searchables.length];
    for (int i = 0; i < searchables.length; i++) {
      queries[i] = searchables[i].rewrite(original);
    }
    return queries[0].combine(queries);
  }

  public Explanation explain(Weight weight, int doc) throws IOException {
    int i = subSearcher(doc);			  // find searcher index
    return searchables[i].explain(weight,doc-starts[i]); // dispatch to searcher
  }

  /**
   * Create weight in multiple index scenario.
   * 
   * Distributed query processing is done in the following steps:
   * 1. rewrite query
   * 2. extract necessary terms
   * 3. collect dfs for these terms from the Searchables
   * 4. create query weight using aggregate dfs.
   * 5. distribute that weight to Searchables
   * 6. merge results
   *
   * Steps 1-4 are done here, 5+6 in the search() methods
   *
   * @return rewritten queries
   */
  protected Weight createWeight(Query original) throws IOException {
    // step 1
    Query rewrittenQuery = rewrite(original);

    // step 2
    Set terms = new HashSet();
    rewrittenQuery.extractTerms(terms);

    // step3
    Term[] allTermsArray = new Term[terms.size()];
    terms.toArray(allTermsArray);
    int[] aggregatedDfs = new int[terms.size()];
    for (int i = 0; i < searchables.length; i++) {
      int[] dfs = searchables[i].docFreqs(allTermsArray);
      for(int j=0; j<aggregatedDfs.length; j++){
        aggregatedDfs[j] += dfs[j];
      }
    }

    HashMap dfMap = new HashMap();
    for(int i=0; i<allTermsArray.length; i++) {
      dfMap.put(allTermsArray[i], new Integer(aggregatedDfs[i]));
    }

    // step4
    int numDocs = maxDoc();
    CachedDfSource cacheSim = new CachedDfSource(dfMap, numDocs);

    return rewrittenQuery.weight(cacheSim);
  }

}
