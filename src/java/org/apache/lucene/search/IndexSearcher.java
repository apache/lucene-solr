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
import java.util.BitSet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.PriorityQueue;

/** Implements search over a single IndexReader. */
public final class IndexSearcher extends Searcher {
  IndexReader reader;

  /** Creates a searcher searching the index in the named directory. */
  public IndexSearcher(String path) throws IOException {
    this(IndexReader.open(path));
  }
    
  /** Creates a searcher searching the index in the provided directory. */
  public IndexSearcher(Directory directory) throws IOException {
    this(IndexReader.open(directory));
  }
    
  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    reader = r;
  }
    
  /** Frees resources associated with this Searcher. */
  public final void close() throws IOException {
    reader.close();
  }

  final int docFreq(Term term) throws IOException {
    return reader.docFreq(term);
  }

  final Document doc(int i) throws IOException {
    return reader.document(i);
  }

  final int maxDoc() throws IOException {
    return reader.maxDoc();
  }

  final TopDocs search(Query query, Filter filter, final int nDocs)
       throws IOException {
    Scorer scorer = Query.scorer(query, this, reader);
    if (scorer == null)
      return new TopDocs(0, new ScoreDoc[0]);

    final BitSet bits = filter != null ? filter.bits(reader) : null;
    final HitQueue hq = new HitQueue(nDocs);
    final int[] totalHits = new int[1];
    scorer.score(new HitCollector() {
	private float minScore = 0.0f;
	public final void collect(int doc, float score) {
	  if (score > 0.0f &&			  // ignore zeroed buckets
	      (bits==null || bits.get(doc))) {	  // skip docs not in bits
	    totalHits[0]++;
	    if (score >= minScore) {
	      hq.put(new ScoreDoc(doc, score));	  // update hit queue
	      if (hq.size() > nDocs) {		  // if hit queue overfull
		hq.pop();			  // remove lowest in hit queue
		minScore = ((ScoreDoc)hq.top()).score; // reset minScore
	      }
	    }
	  }
	}
      }, reader.maxDoc());

    ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
    for (int i = hq.size()-1; i >= 0; i--)	  // put docs in array
      scoreDocs[i] = (ScoreDoc)hq.pop();
    
    return new TopDocs(totalHits[0], scoreDocs);
  }

  /** Lower-level search API.
   *
   * <p>{@link HitCollector#collect(int,float)} is called for every non-zero
   * scoring document.
   *
   * <p>Applications should only use this if they need <it>all</it> of the
   * matching documents.  The high-level search API ({@link
   * Searcher#search(Query)}) is usually more efficient, as it skips
   * non-high-scoring hits.  */
  public final void search(Query query, HitCollector results)
      throws IOException {
    search(query, null, results);
  }

  /** Lower-level search API.
   *
   * <p>{@link HitCollector#collect(int,float)} is called for every non-zero
   * scoring document.
   *
   * <p>Applications should only use this if they need <it>all</it> of the
   * matching documents.  The high-level search API ({@link
   * Searcher#search(Query)}) is usually more efficient, as it skips
   * non-high-scoring hits.  */
  public final void search(Query query, Filter filter,
			   final HitCollector results) throws IOException {
    HitCollector collector = results;
    if (filter != null) {
      final BitSet bits = filter.bits(reader);
      collector = new HitCollector() {
	  public final void collect(int doc, float score) {
	    if (bits.get(doc)) {		  // skip docs not in bits
	      results.collect(doc, score);
	    }
	  }
	};
    }

    Scorer scorer = Query.scorer(query, this, reader);
    if (scorer == null)
      return;
    scorer.score(collector, reader.maxDoc());
  }

}
