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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/**
 * Expert: A hit queue for sorting by hits by terms in more than one field.
 * The type of content in each field could be determined dynamically by
 * FieldSortedHitQueue.determineComparator().
 *
 * <p>Created: Feb 3, 2004 4:46:55 PM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 * @see FieldSortedHitQueue
 * @see Searchable#search(Query,Filter,int,Sort)
 */
class MultiFieldSortedHitQueue
extends PriorityQueue {

	/**
	 * Creates a hit queue sorted by the given list of fields.
	 * @param reader  Index to use.
	 * @param fields Field names, in priority order (highest priority first).  Cannot be <code>null</code> or empty.
	 * @param size  The number of hits to retain.  Must be greater than zero.
	 * @throws IOException
	 */
	MultiFieldSortedHitQueue (IndexReader reader, SortField[] fields, int size)
	throws IOException {
		final int n = fields.length;
		comparators = new ScoreDocComparator[n];
		this.fields = new SortField[n];
		for (int i=0; i<n; ++i) {
			String fieldname = fields[i].getField();
			comparators[i] = FieldSortedHitQueue.getCachedComparator (reader, fieldname, fields[i].getType());
			this.fields[i] = new SortField (fieldname, comparators[i].sortType(), fields[i].getReverse());
		}
		initialize (size);
	}


	/** Stores a comparator corresponding to each field being sorted by */
	protected ScoreDocComparator[] comparators;

	/** Stores the sort criteria being used. */
	protected SortField[] fields;

	/** Stores the maximum score value encountered, for normalizing.
	 *  we only care about scores greater than 1.0 - if all the scores
	 *  are less than 1.0, we don't have to normalize. */
	protected float maxscore = 1.0f;


	/**
	 * Returns whether <code>a</code> is less relevant than <code>b</code>.
	 * @param a ScoreDoc
	 * @param b ScoreDoc
	 * @return <code>true</code> if document <code>a</code> should be sorted after document <code>b</code>.
	 */
	protected final boolean lessThan (final Object a, final Object b) {
		final ScoreDoc docA = (ScoreDoc) a;
		final ScoreDoc docB = (ScoreDoc) b;

		// keep track of maximum score
		if (docA.score > maxscore) maxscore = docA.score;
		if (docB.score > maxscore) maxscore = docB.score;

		// run comparators
		final int n = comparators.length;
		int c = 0;
		for (int i=0; i<n && c==0; ++i) {
			c = (fields[i].reverse) ? comparators[i].compareReverse (docA, docB)
			                        : comparators[i].compare (docA, docB);
		}
		return c > 0;
	}


	/**
	 * Given a FieldDoc object, stores the values used
	 * to sort the given document.  These values are not the raw
	 * values out of the index, but the internal representation
	 * of them.  This is so the given search hit can be collated
	 * by a MultiSearcher with other search hits.
	 * @param  doc  The FieldDoc to store sort values into.
	 * @return  The same FieldDoc passed in.
	 * @see Searchable#search(Query,Filter,int,Sort)
	 */
	FieldDoc fillFields (final FieldDoc doc) {
		final int n = comparators.length;
		final Object[] fields = new Object[n];
		for (int i=0; i<n; ++i)
			fields[i] = comparators[i].sortValue(doc);
		doc.fields = fields;
		if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
		return doc;
	}


	/** Returns the SortFields being used by this hit queue. */
	SortField[] getFields() {
		return fields;
	}

}
