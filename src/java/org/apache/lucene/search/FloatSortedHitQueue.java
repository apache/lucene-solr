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
 * Expert: A sorted hit queue for fields that contain strictly floating point values.
 * Hits are sorted into the queue by the values in the field and then by document number.
 *
 * <p>Created: Feb 2, 2004 9:23:03 AM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
class FloatSortedHitQueue
extends FieldSortedHitQueue {

	/**
	 * Creates a hit queue sorted over the given field containing float values.
	 * @param reader Index to use.
	 * @param float_field Field containing float sort information
	 * @param size Number of hits to collect.
	 * @throws IOException If an error occurs reading the index.
	 */
	FloatSortedHitQueue (IndexReader reader, String float_field, int size)
	throws IOException {
		super (reader, float_field, size);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing floats.
	 * Just calls <code>comparator(IndexReader,String)</code>.
	 * @param reader  Index to use.
	 * @param field  Field containg float values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	protected ScoreDocLookupComparator createComparator (final IndexReader reader, final String field)
	throws IOException {
		return comparator (reader, field);
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing floats.
	 * @param reader  Index to use.
	 * @param fieldname  Field containg float values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final String fieldname)
	throws IOException {
		final String field = fieldname.intern();
		return new ScoreDocLookupComparator () {

			protected final float[] fieldOrder = generateSortIndex();

			protected final float[] generateSortIndex()
			throws IOException {

				float[] retArray = new float[reader.maxDoc()];

				TermEnum enumerator = reader.terms (new Term (field, ""));
				TermDocs termDocs = reader.termDocs ();
				if (enumerator.term () == null) {
					throw new RuntimeException ("no terms in field " + field);
				}

				try {
					do {
						Term term = enumerator.term ();
						if (term.field () != field) break;
						float termval = Float.parseFloat (term.text());
						termDocs.seek (enumerator);
						while (termDocs.next ()) {
							retArray[termDocs.doc ()] = termval;
						}
					} while (enumerator.next ());
				} finally {
					enumerator.close ();
					termDocs.close ();
				}

				return retArray;
			}

			public final int compare (final ScoreDoc i, final ScoreDoc j) {
				final float fi = fieldOrder[i.doc];
				final float fj = fieldOrder[j.doc];
				if (fi < fj) return -1;
				if (fi > fj) return 1;
				return 0;
			}

			public final int compareReverse (final ScoreDoc i, final ScoreDoc j) {
				final float fi = fieldOrder[i.doc];
				final float fj = fieldOrder[j.doc];
				if (fi > fj) return -1;
				if (fi < fj) return 1;
				return 0;
			}

			public final boolean sizeMatches (final int n) {
				return fieldOrder.length == n;
			}

			public Object sortValue (final ScoreDoc i) {
				return new Float (fieldOrder[i.doc]);
			}

			public int sortType() {
				return SortField.FLOAT;
			}
		};
	}


	/**
	 * Returns a comparator for sorting hits according to a field containing floats using the given enumerator
	 * to collect term values.
	 * @param reader  Index to use.
	 * @param fieldname  Field containg float values.
	 * @return  Comparator for sorting hits.
	 * @throws IOException If an error occurs reading the index.
	 */
	static ScoreDocLookupComparator comparator (final IndexReader reader, final TermEnum enumerator, final String fieldname)
	throws IOException {
		final String field = fieldname.intern();
		return new ScoreDocLookupComparator () {

			protected final float[] fieldOrder = generateSortIndex();

			protected final float[] generateSortIndex()
			throws IOException {

				float[] retArray = new float[reader.maxDoc()];

				TermDocs termDocs = reader.termDocs ();
				try {
					do {
						Term term = enumerator.term();
						if (term.field() != field) break;
						float termval = Float.parseFloat (term.text());
						termDocs.seek (enumerator);
						while (termDocs.next()) {
							retArray[termDocs.doc()] = termval;
						}
					} while (enumerator.next());
				} finally {
					termDocs.close();
				}

				return retArray;
			}

			public final int compare (final ScoreDoc i, final ScoreDoc j) {
				final float fi = fieldOrder[i.doc];
				final float fj = fieldOrder[j.doc];
				if (fi < fj) return -1;
				if (fi > fj) return 1;
				return 0;
			}

			public final int compareReverse (final ScoreDoc i, final ScoreDoc j) {
				final float fi = fieldOrder[i.doc];
				final float fj = fieldOrder[j.doc];
				if (fi > fj) return -1;
				if (fi < fj) return 1;
				return 0;
			}

			public final boolean sizeMatches (final int n) {
				return fieldOrder.length == n;
			}

			public Object sortValue (final ScoreDoc i) {
				return new Float (fieldOrder[i.doc]);
			}

			public int sortType() {
				return SortField.FLOAT;
			}
		};
	}
}
