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


/**
 * Expert: Compares two ScoreDoc objects for sorting.
 *
 * <p>Created: Feb 3, 2004 9:00:16 AM 
 * 
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
interface ScoreDocComparator {

	/** Special comparator for sorting hits according to computed relevance (document score). */
	static final ScoreDocComparator RELEVANCE = new ScoreDocComparator() {
		public int compare (ScoreDoc i, ScoreDoc j) {
			if (i.score > j.score) return -1;
			if (i.score < j.score) return 1;
			return 0;
		}
		public int compareReverse (ScoreDoc i, ScoreDoc j) {
			if (i.score < j.score) return -1;
			if (i.score > j.score) return 1;
			return 0;
		}
		public Object sortValue (ScoreDoc i) {
			return new Float (i.score);
		}
		public int sortType() {
			return SortField.SCORE;
		}
	};


	/** Special comparator for sorting hits according to index order (document number). */
	static final ScoreDocComparator INDEXORDER = new ScoreDocComparator() {
		public int compare (ScoreDoc i, ScoreDoc j) {
			if (i.doc < j.doc) return -1;
			if (i.doc > j.doc) return 1;
			return 0;
		}
		public int compareReverse (ScoreDoc i, ScoreDoc j) {
			if (i.doc > j.doc) return -1;
			if (i.doc < j.doc) return 1;
			return 0;
		}
		public Object sortValue (ScoreDoc i) {
			return new Integer (i.doc);
		}
		public int sortType() {
			return SortField.DOC;
		}
	};


	/**
	 * Compares two ScoreDoc objects and returns a result indicating their
	 * sort order.
	 * @param i First ScoreDoc
	 * @param j Second ScoreDoc
	 * @return <code>-1</code> if <code>i</code> should come before <code>j</code><br><code>1</code> if <code>i</code> should come after <code>j</code><br><code>0</code> if they are equal
	 * @see java.util.Comparator
	 */
	int compare (ScoreDoc i, ScoreDoc j);


	/**
	 * Compares two ScoreDoc objects and returns a result indicating their
	 * sort order in reverse.
	 * @param i First ScoreDoc
	 * @param j Second ScoreDoc
	 * @return <code>-1</code> if <code>i</code> should come before <code>j</code><br><code>1</code> if <code>i</code> should come after <code>j</code><br><code>0</code> if they are equal
	 * @see java.util.Comparator
	 */
	int compareReverse (ScoreDoc i, ScoreDoc j);


	/**
	 * Returns the value used to sort the given document.  This is
	 * currently always either an Integer or Float, but could be extended
	 * to return any object used to sort by. 
	 * @param i Document
	 * @return Integer or Float
	 */
	Object sortValue (ScoreDoc i);


	/**
	 * Returns the type of sort.
	 * @return One of the constants in SortField.
	 * @see SortField
	 */
	int sortType();
}