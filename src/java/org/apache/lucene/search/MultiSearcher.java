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

/** Implements search over a set of Searcher's. */
public final class MultiSearcher extends Searcher {
  private Searcher[] searchers;
  private int[] starts;
  private int maxDoc = 0;

  /** Creates a searcher which searches <i>searchers</i>. */
  public MultiSearcher(Searcher[] searchers) throws IOException {
    this.searchers = searchers;

    starts = new int[searchers.length + 1];	  // build starts array
    for (int i = 0; i < searchers.length; i++) {
      starts[i] = maxDoc;
      maxDoc += searchers[i].maxDoc();		  // compute maxDocs
    }
    starts[searchers.length] = maxDoc;
  }
    
  /** Frees resources associated with this Searcher. */
  public final void close() throws IOException {
    for (int i = 0; i < searchers.length; i++)
      searchers[i].close();
  }

  final int docFreq(Term term) throws IOException {
    int docFreq = 0;
    for (int i = 0; i < searchers.length; i++)
      docFreq += searchers[i].docFreq(term);
    return docFreq;
  }

  final Document doc(int n) throws IOException {
    int i = searcherIndex(n);			  // find searcher index
    return searchers[i].doc(n - starts[i]);	  // dispatch to searcher
  }

  // replace w/ call to Arrays.binarySearch in Java 1.2
  private final int searcherIndex(int n) {	  // find searcher for doc n:
    int lo = 0;					  // search starts array
    int hi = searchers.length - 1;		  // for first element less
						  // than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >> 1;
      int midValue = starts[mid];
      if (n < midValue)
	hi = mid - 1;
      else if (n > midValue)
	lo = mid + 1;
      else
	return mid;
    }
    return hi;
  }

  final int maxDoc() throws IOException {
    return maxDoc;
  }

  final TopDocs search(Query query, Filter filter, int nDocs)
       throws IOException {
    HitQueue hq = new HitQueue(nDocs);
    float minScore = 0.0f;
    int totalHits = 0;

    for (int i = 0; i < searchers.length; i++) {  // search each searcher
      TopDocs docs = searchers[i].search(query, filter, nDocs);
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
}
