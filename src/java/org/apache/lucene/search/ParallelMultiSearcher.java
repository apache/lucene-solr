package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
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

import org.apache.lucene.index.Term;

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
		int docFreq = 0;
		for (int i = 0; i < searchables.length; i++)
			docFreq += searchables[i].docFreq(term);
		return docFreq;
	}

	/**
	* A search implementation which spans a new thread for each
	* Searchable, waits for each search to complete and merge
	* the results back together.
	*/
	public TopDocs search(Query query, Filter filter, int nDocs)
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
					query,
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
	 * 
	 * TODO: parallelize this one too
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

	/*
	 * TODO: this one could be parallelized too
	 * @see org.apache.lucene.search.Searchable#rewrite(org.apache.lucene.search.Query)
	 */
	public Query rewrite(Query original) throws IOException {
		Query[] queries = new Query[searchables.length];
		for (int i = 0; i < searchables.length; i++) {
			queries[i] = searchables[i].rewrite(original);
		}
		return original.combine(queries);
	}

}

/**
 * A thread subclass for searching a single searchable 
 */
class MultiSearcherThread extends Thread {

	private Searchable searchable;
	private Query query;
	private Filter filter;
	private int nDocs;
	private int hits;
	private TopDocs docs;
	private int i;
	private HitQueue hq;
	private int[] starts;
	private IOException ioe;

	public MultiSearcherThread(
		Searchable searchable,
		Query query,
		Filter filter,
		int nDocs,
		HitQueue hq,
		int i,
		int[] starts,
		String name) {
		super(name);
		this.searchable = searchable;
		this.query = query;
		this.filter = filter;
		this.nDocs = nDocs;
		this.hq = hq;
		this.i = i;
		this.starts = starts;
	}

	public void run() {
		try {
			docs = searchable.search(query, filter, nDocs);
		}
		// Store the IOException for later use by the caller of this thread
		catch (IOException ioe) {
			this.ioe = ioe;
		}
		if (ioe == null) {
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

	public IOException getIOException() {
		return ioe;
	}

}
