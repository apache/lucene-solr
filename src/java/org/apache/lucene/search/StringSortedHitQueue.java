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
 * Warning: The internal cache could be quite large, depending on the number of terms
 * in the field!  All the terms are kept in memory, as well as a sorted array of
 * integers representing their relative position.
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
	 * @param fieldname  Field containg string values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final String fieldname)
	throws IOException {
		TermEnum enumerator = reader.terms (new Term (fieldname, ""));
		return comparator (reader, enumerator, fieldname);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing strings using the given enumerator
	 * to collect term values.
	 * @param reader  Index to use.
	 * @param fieldname  Field containg string values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final TermEnum enumerator, final String fieldname)
	throws IOException {
		final String field = fieldname.intern();
		return new ScoreDocLookupComparator() {

			protected final int[] fieldOrder = generateSortIndex();
			protected String[] terms;

			private final int[] generateSortIndex()
			throws IOException {

				final int[] retArray = new int[reader.maxDoc()];
				final String[] mterms = new String[reader.maxDoc()];  // guess length
				if (retArray.length > 0) {
					TermDocs termDocs = reader.termDocs();
					int t = 0;  // current term number
					try {
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

						// if a given document has more than one term
						// in the field, only the last one will be used.

						do {
							Term term = enumerator.term();
							if (term.field() != field) break;

							// store term text
							// we expect that there is at most one term per document
							if (t >= mterms.length) throw new RuntimeException ("there are more terms than documents in field \""+field+"\"");
							mterms[t] = term.text();

							// store which documents use this term
							termDocs.seek (enumerator);
							while (termDocs.next()) {
								retArray[termDocs.doc()] = t;
							}

							t++;
						} while (enumerator.next());
					} finally {
						termDocs.close();
					}

					// if there are less terms than documents,
					// trim off the dead array space
					if (t < mterms.length) {
						terms = new String[t];
						System.arraycopy (mterms, 0, terms, 0, t);
					} else {
						terms = mterms;
					}
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
				return terms[fieldOrder[i.doc]];
			}

			public int sortType() {
				return SortField.STRING;
			}
		};
	}
}
