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

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

import java.util.HashMap;
import java.io.IOException;

/**
 * Expert: collects results from a search and sorts them by terms in a
 * given field in each document.
 *
 * <p>In this version (0.1) the field to sort by must contain strictly
 * String representations of Integers.
 * See {@link SortedIndexSearcher SortedIndexSearcher} for more
 * information.  Each document is assumed to have a single term in the
 * given field, and the value of the term is the document's relative
 * position in the given sort order.
 *
 * <p>When one of these objects is created, a TermEnumerator is
 * created to fetch all the terms in the index for the given field.
 * The value of each term is assumed to be an integer representing a
 * sort position.  Each document is assumed to contain one of the
 * terms, indicating where in the sort it belongs.
 *
 * <p><h3>Memory Usage</h3>
 *
 * <p>A static cache is maintained.  This cache contains an integer
 * array of length <code>IndexReader.maxDoc()</code> for each field
 * name for which a sort is performed.  In other words, the size of
 * the cache in bytes is:
 *
 * <p><code>4 * IndexReader.maxDoc() * (# of different fields actually used to sort)</code>
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
 * @author  "Tim Jones" &lt;tjluc@nacimiento.com&gt;
 * @since   lucene 1.3
 * @version 0.1
 */
public class FieldSortedHitQueue
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
     * Contains the cache of sort information.  The
     * key is field name, the value an array of int.
     * A HashMap is used, and we are careful how we
     * handle synchronization.  This is because best
     * performance is obtained when the same IndexReader
     * is used over and over, and we therefore perform
     * many reads and few writes.
     */
    protected static HashMap fieldCache;

    /** The sort information being used by this instance */
    protected int[] fieldOrder;

    /**
     * Creates a hit queue sorted by the given field.
     * @param reader  IndexReader to use.
     * @param integer_field  Field to sort by.
     * @param size    Number of hits to return - see {@link PriorityQueue#initialize(int) initialize}
     * @throws IOException  If the internal term enumerator fails.
     */
    public FieldSortedHitQueue (IndexReader reader, String integer_field, int size)
    throws IOException {

        int hash = reader.hashCode();
        if (hash != lastReaderHash) {
            lastReaderHash = hash;
            if (fieldCache != null) {
                fieldCache.clear();
            }
            fieldCache = new HashMap();
        }

        initialize (size);
        initializeSort (reader, integer_field);
    }

    /**
     * Compares documents based on the value of the term in the field
     * being sorted by.  Documents which should appear at the top of the
     * list should have low values in the term; documents which should
     * appear at the end should have high values.
     *
     * <p>In the context of this method, "less than" means "less relevant",
     * so documents at the top of the list are "greatest" and documents at
     * the bottom are "least".
     *
     * <p>Document A is considered less than Document B
     * if A.field.term > B.field.term or A.doc > B.doc.
     *
     * @param a  ScoreDoc object for document a.
     * @param b  ScoreDoc object for document b.
     * @return true if document a is less than document b.
     * @see ScoreDoc
     */
    protected final boolean lessThan (Object a, Object b) {
        ScoreDoc hitA = (ScoreDoc) a;
        ScoreDoc hitB = (ScoreDoc) b;
        int scoreA = fieldOrder[hitA.doc];
        int scoreB = fieldOrder[hitB.doc];
        if (scoreA == scoreB)
            return hitA.doc > hitB.doc;
        else
            return scoreA > scoreB;   // bigger is really less - the ones at the top should be the lowest
    }

    /**
     * Initializes the cache of sort information.  <code>fieldCache</code> is queried
     * to see if it has the term information for the given field.
     * If so, and if the reader still has the same value for maxDoc()
     * (note that we assume new IndexReaders are caught during the
     * constructor), the existing data is used.  If not, all the term values
     * for the given field are fetched.  The value of the term is assumed
     * to be the sort index for any documents containing the term.  Documents
     * should only have one term in the given field. Multiple documents
     * can share the same term if desired (documents with the same term will
     * be sorted relative to each other by the order they were placed in
     * the index).
     * @param reader  The document index.
     * @param field   The field to sort by.
     * @throws IOException  If the term enumerator fails.
     */
    protected final void initializeSort (IndexReader reader, String field)
    throws IOException {

        fieldOrder = (int[]) fieldCache.get (field);
        if (fieldOrder == null || fieldOrder.length != reader.maxDoc()) {
            fieldOrder = new int [reader.maxDoc()];

            TermEnum enumerator = reader.terms (new Term (field, ""));
            TermDocs termDocs = reader.termDocs();
            if (enumerator.term() == null) {
                throw new RuntimeException ("no terms in field "+field);
            }

            try {
                Term term = enumerator.term();
                while (term.field() == field) {
                    termDocs.seek (term);
                    if (termDocs.next()) {
                        fieldOrder[termDocs.doc()] = Integer.parseInt (term.text());
                    } else {
                        throw new RuntimeException ("termDocs.next() failed!");
                    }
                    if (!enumerator.next()) {
                        break;
                    }
                    term = enumerator.term();
                }
            } finally {
                enumerator.close();
                termDocs.close();
            }

            // be careful how the cache is updated so we
            // don't have synchronization problems.  we do
            // it this way because we assume updates will be
            // few compared to the number of reads.
            HashMap newCache = (HashMap) fieldCache.clone();
            newCache.put (field, fieldOrder);
            fieldCache = newCache;
        }
    }
}
