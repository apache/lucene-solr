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
 * Expert: A sorted hit queue for fields that contain strictly integer values.
 * Hits are sorted into the queue by the values in the field and then by document number.
 *
 * <p>Created: Jan 30, 2004 3:35:09 PM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
class IntegerSortedHitQueue
extends FieldSortedHitQueue {

	/**
	 * Creates a hit queue sorted over the given field containing integer values.
	 * @param reader Index to use.
	 * @param integer_field Field containing integer sort information
	 * @param size Number of hits to collect.
	 * @throws IOException If an error occurs reading the index.
	 */
	IntegerSortedHitQueue (IndexReader reader, String integer_field, int size)
	throws IOException {
		super (reader, integer_field, size);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing integers.
	 * Just calls <code>comparator(IndexReader,String)</code>.
	 * @param reader  Index to use.
	 * @param field  Field containg integer values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	protected ScoreDocLookupComparator createComparator (final IndexReader reader, final String field)
	throws IOException {
		return comparator (reader, field);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing integers.
	 * @param reader  Index to use.
	 * @param fieldname  Field containg integer values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final String fieldname)
	throws IOException {
		TermEnum enumerator = reader.terms (new Term (fieldname, ""));
		return comparator (reader, enumerator, fieldname);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing integers using the given enumerator
	 * to collect term values.
	 * @param reader  Index to use.
	 * @param fieldname  Field containg integer values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final TermEnum enumerator, final String fieldname)
	throws IOException {
		final String field = fieldname.intern();
		return new ScoreDocLookupComparator() {

			protected final int[] fieldOrder = generateSortIndex();

			private final int[] generateSortIndex()
			throws IOException {

				final int[] retArray = new int[reader.maxDoc()];
				if (retArray.length > 0) {
					TermDocs termDocs = reader.termDocs();
					try {
						if (enumerator.term() == null) {
							throw new RuntimeException ("no terms in field "+field);
						}
						do {
							Term term = enumerator.term();
							if (term.field() != field) break;
							int termval = Integer.parseInt (term.text());
							termDocs.seek (enumerator);
							while (termDocs.next()) {
								retArray[termDocs.doc()] = termval;
							}
						} while (enumerator.next());
					} finally {
						termDocs.close();
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
				return new Integer (fieldOrder[i.doc]);
			}

			public int sortType() {
				return SortField.INT;
			}
		};
	}
}
