package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.util.Vector;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.PriorityQueue;

/** Implements search over a set of <code>Searchables</code>.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query)}
 * or {@link #search(Query,Filter)} methods.
 */
public class MultiSearcher extends Searcher implements Searchable {
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

  /** Frees resources associated with this <code>Searcher</code>. */
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

  /** For use by {@link HitCollector} implementations. */
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
    float minScore = 0.0f;
    int totalHits = 0;

    for (int i = 0; i < searchables.length; i++) { // search each searcher
      TopDocs docs = searchables[i].search(query, filter, nDocs);
      totalHits += docs.totalHits;		  // update totalHits
      ScoreDoc[] scoreDocs = docs.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) { // merge scoreDocs into hq
	ScoreDoc scoreDoc = scoreDocs[j];
	if (scoreDoc.score >= minScore) {
	  scoreDoc.doc += starts[i];		  // convert doc
	  hq.put(scoreDoc);			  // update hit queue
	  if (hq.size() > nDocs) {		  // if hit queue overfull
	    hq.pop();				  // remove lowest in hit queue
	    minScore = ((ScoreDoc)hq.top()).score; // reset minScore
	  }
	} else
	  break;				  // no more scores > minScore
      }
    }

    ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size()-1; i >= 0; i--)	  // put docs in array
      scoreDocs[i] = (ScoreDoc)hq.pop();

    return new TopDocs(totalHits, scoreDocs);
  }


  /** Lower-level search API.
   *
   * <p>{@link HitCollector#collect(int,float)} is called for every non-zero
   * scoring document.
   *
   * <p>Applications should only use this if they need <i>all</i> of the
   * matching documents.  The high-level search API ({@link
   * Searcher#search(Query)}) is usually more efficient, as it skips
   * non-high-scoring hits.
   *
   * @param query to match documents
   * @param filter if non-null, a bitset used to eliminate some documents
   * @param results to receive hits
   */
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
  
  /** */
  public Query rewrite(Query original) throws IOException {
    Query[] queries = new Query[searchables.length];
    for (int i = 0; i < searchables.length; i++) {
      queries[i] = searchables[i].rewrite(original);
    }
    return original.combine(queries);
  }


  /** */
  public Explanation explain(Query query, int doc) throws IOException {
    int i = subSearcher(doc);			  // find searcher index
    return searchables[i].explain(query,doc-starts[i]); // dispatch to searcher
  }

}
