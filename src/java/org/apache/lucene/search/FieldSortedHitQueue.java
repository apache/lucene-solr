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
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Hashtable;

/**
 * Expert: Base class for collecting results from a search and sorting
 * them by terms in a given field in each document.
 *
 * <p>When one of these objects is created, a TermEnumerator is
 * created to fetch all the terms in the index for the given field.
 * The value of each term is assumed to represent a
 * sort position.  Each document is assumed to contain one of the
 * terms, indicating where in the sort it belongs.
 *
 * <p><h3>Memory Usage</h3>
 *
 * <p>A static cache is maintained.  This cache contains an integer
 * or float array of length <code>IndexReader.maxDoc()</code> for each field
 * name for which a sort is performed.  In other words, the size of the
 * cache in bytes is:
 *
 * <p><code>4 * IndexReader.maxDoc() * (# of different fields actually used to sort)</code>
 *
 * <p>For String fields, the cache is larger: in addition to the
 * above array, the value of every term in the field is kept in memory.
 * If there are many unique terms in the field, this could 
 * be quite large.
 *
 * <p>Note that the size of the cache is not affected by how many
 * fields are in the index and <i>might</i> be used to sort - only by
 * the ones actually used to sort a result set.
 *
 * <p>The cache is cleared each time a new <code>IndexReader</code> is
 * passed in, or if the value returned by <code>maxDoc()</code>
 * changes for the current IndexReader.  This class is not set up to
 * be able to efficiently sort hits from more than one index
 * simultaneously.
 *
 * <p>Created: Dec 8, 2003 12:56:03 PM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
abstract class FieldSortedHitQueue
extends PriorityQueue {

    /**
     * Keeps track of the IndexReader which the cache
     * applies to.  If it changes, the cache is cleared.
     * We only store the hashcode so as not to mess up
     * garbage collection by having a reference to an
     * IndexReader.
     */
    protected static int lastReaderHash;

    /**
	 * Contains the cache of sort information, mapping
	 * String (field names) to ScoreDocComparator.
	 */
    protected static final Hashtable fieldCache = new Hashtable();

	/** The pattern used to detect integer values in a field */
	/** removed for java 1.3 compatibility
		protected static final Pattern pIntegers = Pattern.compile ("[0-9\\-]+");
	**/

	/** The pattern used to detect float values in a field */
	/** removed for java 1.3 compatibility
		protected static final Object pFloats = Pattern.compile ("[0-9+\\-\\.eEfFdD]+");
	**/


	/**
	 * Returns a comparator for the given field.  If there is already one in the cache, it is returned.
	 * Otherwise one is created and put into the cache.  If <code>reader</code> is different than the
	 * one used for the current cache, or has changed size, the cache is cleared first.
	 * @param reader  Index to use.
	 * @param field   Field to sort by.
	 * @return  Comparator; never <code>null</code>.
	 * @throws IOException  If an error occurs reading the index.
	 * @see #determineComparator
	 */
	static ScoreDocComparator getCachedComparator (final IndexReader reader, final String field, final int type, final SortComparatorSource factory)
	throws IOException {

		if (type == SortField.DOC) return ScoreDocComparator.INDEXORDER;
		if (type == SortField.SCORE) return ScoreDocComparator.RELEVANCE;

		// see if we have already generated a comparator for this field
		if (reader.hashCode() == lastReaderHash) {
			ScoreDocLookupComparator comparer = (ScoreDocLookupComparator) fieldCache.get (field);
			if (comparer != null && comparer.sizeMatches(reader.maxDoc())) {
				return comparer;
			}
		} else {
			lastReaderHash = reader.hashCode();
			fieldCache.clear();
		}

		ScoreDocComparator comparer = null;
		switch (type) {
			case SortField.SCORE:  comparer = ScoreDocComparator.RELEVANCE; break;
			case SortField.DOC:    comparer = ScoreDocComparator.INDEXORDER; break;
			case SortField.AUTO:   comparer = determineComparator (reader, field); break;
			case SortField.STRING: comparer = StringSortedHitQueue.comparator (reader, field); break;
			case SortField.INT:    comparer = IntegerSortedHitQueue.comparator (reader, field); break;
			case SortField.FLOAT:  comparer = FloatSortedHitQueue.comparator (reader, field); break;
			case SortField.CUSTOM: comparer = factory.newComparator (reader, field); break;
			default:
				throw new RuntimeException ("invalid sort field type: "+type);
		}

		// store the comparator in the cache for reuse
		fieldCache.put (field, comparer);

		return comparer;
	}


	/** Clears the static cache of sorting information. */
	static void clearCache() {
		fieldCache.clear();
	}


	/**
	 * Returns a FieldSortedHitQueue sorted by the given ScoreDocComparator.
	 * @param comparator Comparator to use.
	 * @param size       Number of hits to retain.
	 * @return  Hit queue sorted using the given comparator.
	 */
	static FieldSortedHitQueue getInstance (ScoreDocComparator comparator, int size) {
		return new FieldSortedHitQueue (comparator, size) {
			// dummy out the abstract method
			protected ScoreDocLookupComparator createComparator (IndexReader reader, String field) throws IOException {
				return null;
			}
		};
	}


	/**
	 * Looks at the actual values in the field and determines whether
	 * they contain Integers, Floats or Strings.  Only the first term in the field
	 * is looked at.
	 * <p>The following patterns are used to determine the content of the terms:
	 * <p><table border="1" cellspacing="0" cellpadding="3">
	 * <tr><th>Sequence</th><th>Pattern</th><th>Type</th></tr>
	 * <tr><td>1</td><td>[0-9\-]+</td><td>Integer</td></tr>
	 * <tr><td>2</td><td>[0-9+\-\.eEfFdD]+</td><td>Float</td></tr>
	 * <tr><td>3</td><td><i>(none - default)</i></td><td>String</td></tr>
	 * </table>
	 *
	 * @param reader  Index to use.
	 * @param field   Field to create comparator for.
	 * @return  Comparator appropriate for the terms in the given field.
	 * @throws IOException  If an error occurs reading the index.
	 */
	protected static ScoreDocComparator determineComparator (IndexReader reader, String field)
	throws IOException {
		field = field.intern();
		TermEnum enumerator = reader.terms (new Term (field, ""));
		try {
			Term term = enumerator.term();
			if (term == null) {
				throw new RuntimeException ("no terms in field "+field+" - cannot determine sort type");
			}
			if (term.field() == field) {
				String termtext = term.text().trim();

				/**
				 * Java 1.4 level code:

				if (pIntegers.matcher(termtext).matches())
					return IntegerSortedHitQueue.comparator (reader, enumerator, field);

				else if (pFloats.matcher(termtext).matches())
					return FloatSortedHitQueue.comparator (reader, enumerator, field);
				 */

				// Java 1.3 level code:
				try {
					Integer.parseInt (termtext);
					return IntegerSortedHitQueue.comparator (reader, enumerator, field);
				} catch (NumberFormatException nfe) {
					// nothing
				}
				try {
					Float.parseFloat (termtext);
					return FloatSortedHitQueue.comparator (reader, enumerator, field);
				} catch (NumberFormatException nfe) {
					// nothing
				}
				
				return StringSortedHitQueue.comparator (reader, enumerator, field);

			} else {
				throw new RuntimeException ("field \""+field+"\" does not appear to be indexed");
			}
		} finally {
			enumerator.close();
		}
	}

	/**
	 * The sorting priority used.  The first element is set by the constructors.
	 * The result is that sorting is done by field value, then by index order.
	 */
	private final ScoreDocComparator[] comparators = new ScoreDocComparator[] {
		null, ScoreDocComparator.INDEXORDER
	};


    /**
     * Creates a hit queue sorted by the given field.  Hits are sorted by the field, then
	 * by index order.
     * @param reader  IndexReader to use.
     * @param field   Field to sort by.
     * @param size    Number of hits to return - see {@link PriorityQueue#initialize(int) initialize}
     * @throws IOException  If the internal term enumerator fails.
     */
    FieldSortedHitQueue (IndexReader reader, String field, int size)
    throws IOException {

		// reset the cache if we have a new reader
        int hash = reader.hashCode();
        if (hash != lastReaderHash) {
            lastReaderHash = hash;
            fieldCache.clear();
        }

		// initialize the PriorityQueue
        initialize (size);

		// set the sort
        comparators[0] = initializeSort (reader, field);
    }


	/**
	 * Creates a sorted hit queue based on an existing comparator.  The hits
	 * are sorted by the given comparator, then by index order.
	 * @param comparator  Comparator used to sort hits.
	 * @param size        Number of hits to retain.
	 */
	protected FieldSortedHitQueue (ScoreDocComparator comparator, int size) {
		initialize (size);          // initialize the PriorityQueue
		comparators[0] = comparator;    // set the sort
	}


	/**
	 * Returns whether <code>a</code> is less relevant than <code>b</code>
	 * @param a ScoreDoc
	 * @param b ScoreDoc
	 * @return <code>true</code> if document <code>a</code> should be sorted after document <code>b</code>.
	 */
	protected final boolean lessThan (final Object a, final Object b) {
		final ScoreDoc docA = (ScoreDoc) a;
		final ScoreDoc docB = (ScoreDoc) b;
		final int n = comparators.length;
		int c = 0;
		for (int i=0; i<n && c==0; ++i) {
			c = comparators[i].compare (docA, docB);
		}
		return c > 0;
	}


    /**
     * Initializes the cache of sort information.  <code>fieldCache</code> is queried
     * to see if it has the term information for the given field.
     * If so, and if the reader still has the same value for maxDoc()
     * (note that we assume new IndexReaders are caught during the
     * constructor), the existing data is used.  If not, all the term values
     * for the given field are fetched.  The value of the term is assumed
     * to indicate the sort order for any documents containing the term.  Documents
     * should only have one term in the given field.  Multiple documents
     * can share the same term if desired, in which case they will be
	 * considered equal during the sort.
     * @param reader  The document index.
     * @param field   The field to sort by.
     * @throws IOException  If createComparator(IndexReader,String) fails - usually caused by the term enumerator failing.
     */
    protected final ScoreDocComparator initializeSort (IndexReader reader, String field)
    throws IOException {

		ScoreDocLookupComparator comparer = (ScoreDocLookupComparator) fieldCache.get (field);
		if (comparer == null || !comparer.sizeMatches(reader.maxDoc())) {
			comparer = createComparator (reader, field);
            fieldCache.put (field, comparer);
		}
		return comparer;
    }


	/**
	 * Subclasses should implement this method to provide an appropriate ScoreDocLookupComparator.
	 * @param reader  Index to use.
	 * @param field   Field to use for sorting.
	 * @return Comparator to use to sort hits.
	 * @throws IOException  If an error occurs reading the index.
	 */
	protected abstract ScoreDocLookupComparator createComparator (IndexReader reader, String field)
	throws IOException;
}