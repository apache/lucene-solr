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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;

/**
 * Expert: A sorted hit queue for fields that contain string values.
 * Hits are sorted into the queue by the values in the field and then by document number.
 * The internal cache contains integers - the strings are sorted and
 * then only their sequence number cached.
 *
 * <p>Created: Feb 2, 2004 9:26:33 AM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
class StringSortedHitQueue
extends FieldSortedHitQueue {

	/**
	 * Creates a hit queue sorted over the given field containing string values.
	 * @param reader Index to use.
	 * @param string_field Field containing string sort information
	 * @param size Number of hits to collect.
	 * @throws IOException If an error occurs reading the index.
	 */
	StringSortedHitQueue (IndexReader reader, String string_field, int size)
	throws IOException {
		super (reader, string_field, size);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing strings.
	 * Just calls <code>comparator(IndexReader,String)</code>.
	 * @param reader  Index to use.
	 * @param field  Field containg string values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	protected ScoreDocLookupComparator createComparator (final IndexReader reader, final String field)
	throws IOException {
		return comparator (reader, field);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing strings.
	 * @param reader  Index to use.
	 * @param field  Field containg string values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final String field)
	throws IOException {
		return new ScoreDocLookupComparator() {

			/** The sort information being used by this instance */
			protected final int[] fieldOrder = generateSortIndex();

			private final int[] generateSortIndex()
			throws IOException {

				final int[] retArray = new int[reader.maxDoc()];

				TermEnum enumerator = reader.terms (new Term (field, ""));
				TermDocs termDocs = reader.termDocs();
				if (enumerator.term() == null) {
					throw new RuntimeException ("no terms in field " + field);
				}

				// NOTE: the contract for TermEnum says the
				// terms will be in natural order (which is
				// ordering by field name, term text).  The
				// contract for TermDocs says the docs will
				// be ordered by document number.  So the
				// following loop will automatically sort the
				// terms in the correct order.

				try {
					int t = 0;  // current term number
					do {
						Term term = enumerator.term();
						if (term.field() != field) break;
						t++;
						termDocs.seek (enumerator);
						while (termDocs.next()) {
							retArray[termDocs.doc()] = t;
						}
					} while (enumerator.next());
				} finally {
					enumerator.close();
					termDocs.close();
				}

				return retArray;
			}

			public final int compare (final ScoreDoc i, final ScoreDoc j) {
				final int fi = fieldOrder[i.doc];
				final int fj = fieldOrder[j.doc];
				if (fi < fj) return -1;
				if (fi > fj) return 1;
				return 0;
			}

			public final int compareReverse (final ScoreDoc i, final ScoreDoc j) {
				final int fi = fieldOrder[i.doc];
				final int fj = fieldOrder[j.doc];
				if (fi > fj) return -1;
				if (fi < fj) return 1;
				return 0;
			}

			public final boolean sizeMatches (final int n) {
				return fieldOrder.length == n;
			}

			public Object sortValue (final ScoreDoc i) {
				return new Integer(fieldOrder[i.doc]);
			}

			public int sortType() {
				return SortField.INT;
			}
		};
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing strings using the given enumerator
	 * to collect term values.
	 * @param reader  Index to use.
	 * @param field  Field containg string values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final TermEnum enumerator, final String field)
	throws IOException {
		return new ScoreDocLookupComparator() {

			protected final int[] fieldOrder = generateSortIndex();

			private final int[] generateSortIndex()
			throws IOException {

				final int[] retArray = new int[reader.maxDoc()];

				// NOTE: the contract for TermEnum says the
				// terms will be in natural order (which is
				// ordering by field name, term text).  The
				// contract for TermDocs says the docs will
				// be ordered by document number.  So the
				// following loop will automatically sort the
				// terms in the correct order.

				TermDocs termDocs = reader.termDocs();
				try {
					int t = 0;  // current term number
					do {
						Term term = enumerator.term();
						if (term.field() != field) break;
						t++;
						termDocs.seek (enumerator);
						while (termDocs.next()) {
							retArray[termDocs.doc()] = t;
						}
					} while (enumerator.next());
				} finally {
					termDocs.close();
				}

				return retArray;
			}

			public final int compare (final ScoreDoc i, final ScoreDoc j) {
				final int fi = fieldOrder[i.doc];
				final int fj = fieldOrder[j.doc];
				if (fi < fj) return -1;
				if (fi > fj) return 1;
				return 0;
			}

			public final int compareReverse (final ScoreDoc i, final ScoreDoc j) {
				final int fi = fieldOrder[i.doc];
				final int fj = fieldOrder[j.doc];
				if (fi > fj) return -1;
				if (fi < fj) return 1;
				return 0;
			}

			public final boolean sizeMatches (final int n) {
				return fieldOrder.length == n;
			}

			public Object sortValue (final ScoreDoc i) {
				return new Integer(fieldOrder[i.doc]);
			}

			public int sortType() {
				return SortField.INT;
			}
		};
	}
}
