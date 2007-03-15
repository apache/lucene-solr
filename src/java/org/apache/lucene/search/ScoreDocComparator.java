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


/**
 * Expert: Compares two ScoreDoc objects for sorting.
 *
 * <p>Created: Feb 3, 2004 9:00:16 AM 
 * 
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
public interface ScoreDocComparator {

	/** Special comparator for sorting hits according to computed relevance (document score). */
	static final ScoreDocComparator RELEVANCE = new ScoreDocComparator() {
		public int compare (ScoreDoc i, ScoreDoc j) {
			if (i.score > j.score) return -1;
			if (i.score < j.score) return 1;
			return 0;
		}
		public Comparable sortValue (ScoreDoc i) {
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
		public Comparable sortValue (ScoreDoc i) {
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
	 * @return a negative integer if <code>i</code> should come before <code>j</code><br>
     *         a positive integer if <code>i</code> should come after <code>j</code><br>
     *         <code>0</code> if they are equal
	 * @see java.util.Comparator
	 */
	int compare (ScoreDoc i, ScoreDoc j);

	/**
	 * Returns the value used to sort the given document.  The
	 * object returned must implement the java.io.Serializable
	 * interface.  This is used by multisearchers to determine how
     * to collate results from their searchers.
	 * @see FieldDoc
	 * @param i Document
	 * @return Serializable object
	 */
	Comparable sortValue (ScoreDoc i);

	/**
	 * Returns the type of sort.  Should return <code>SortField.SCORE</code>,
     * <code>SortField.DOC</code>, <code>SortField.STRING</code>,
     * <code>SortField.INTEGER</code>, <code>SortField.FLOAT</code> or
     * <code>SortField.CUSTOM</code>.  It is not valid to return
     * <code>SortField.AUTO</code>.
     * This is used by multisearchers to determine how to collate results
     * from their searchers.
	 * @return One of the constants in SortField.
	 * @see SortField
	 */
	int sortType();
}
