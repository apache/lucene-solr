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

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.search.*;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.BitSet;

/**
 * Implements search over an IndexReader using the values of terms in
 * a field as the primary sort order.  Secondary sort is by the order
 * of documents in the index.
 *
 * <p>In this version (0.1) the field to sort by must contain strictly
 * String representations of Integers (i.e. {@link Integer#toString Integer.toString()}).
 *
 * Each document is assumed to have a single term in the given field,
 * and the value of the term is the document's relative position in
 * the given sort order.  The field must be indexed, but should not be
 * stored or tokenized:
 *
 * <p><code>document.add(new Field("byAlpha", Integer.toString(x), false, true, false));</code>
 *
 * <p>In other words, the desired order of documents must be encoded
 * at the time they are entered into the index.  The first document
 * should have a low value integer, the last document a high value
 * (i.e. the documents should be numbered <code>1..n</code> where
 * <code>1</code> is the first and <code>n</code> the last).  Values
 * must be between <code>Integer.MIN_VALUE</code> and
 * <code>Integer.MAX_VALUE</code> inclusive.
 *
 * <p>Then, at search time, the field is designated to be used to sort
 * the returned hits:
 *
 * <p><code>IndexSearcher searcher = new IntegerSortedSearcher(indexReader, "byAlpha");</code>
 *
 * <p>or:
 *
 * <p><code>IntegerSortedSearcher searcher = new IntegerSortedSearcher(indexReader, "bySomething");
 * <br>Hits hits = searcher.search(query, filter);
 * <br>...
 * <br>searcher.setOrderByField("bySomethingElse");
 * <br>hits = searcher.search(query, filter);
 * <br>...
 * </code>
 *
 * <p>Note the above example shows that one of these objects can be
 * used multiple times, and the sort order changed between usages.
 *
 * <p><h3>Memory Usage</h3>
 *
 * <p>This object is almost identical to the regular IndexSearcher and
 * makes no additional memory requirements on its own.  Every time the
 * <code>search()</code> method is called, however, a new
 * {@link FieldSortedHitQueue FieldSortedHitQueue} object is created.
 * That object is responsible for putting the hits in the correct order,
 * and it maintains a cache of information based on the IndexReader
 * given to it.  See its documentation for more information on its
 * memory usage.
 *
 * <p><h3>Concurrency</h3>
 *
 * <p>This object has the same behavior during concurrent updates to
 * the index as does IndexSearcher.  Namely, in the default
 * implementation using
 * {@link org.apache.lucene.store.FSDirectory FSDirectory}, the index
 * can be updated (deletes, adds) without harm while this object
 * exists, but this object will not see the changes.  Ultimately this
 * behavior is a result of the
 * {@link org.apache.lucene.index.SegmentReader SegmentReader} class
 * internal to FSDirectory, which caches information about documents
 * in memory.
 *
 * <p>So, in order for IntegerSortedSearcher to be kept up to date with
 * changes to the index, new instances must be created instead of the
 * same one used over and over again.  This will result in lower
 * performance than if instances are reused.
 *
 * <p><h3>Updates</h3>
 *
 * <p>In order to be able to update the index without having to
 * recalculate all the sort numbers, the numbers should be stored with
 * "space" between them.  That is, sort the documents and number them
 * <code>1..n</code>.  Then, as <code>i</code> goes between
 * <code>1</code> and <code>n</code>:
 *
 * <p><code>document.add(new Field("byAlpha", Integer.toString(i*1000), false, true, false));</code>
 *
 * <p>Add a new document sorted between position 1 and 2 by:
 *
 * <p><code>document.add(new Field("byAlpha", Integer.toString(1500), false, true, false));</code>
 *
 * <p>Be careful not to overun <code>Integer.MAX_VALUE</code>
 * (<code>2147483647</code>).  Periodically a complete reindex should
 * be run so the sort orders can be "normalized".
 *
 * <p>Created: Dec 8, 2003 12:47:26 PM
 *
 * @author  "Tim Jones" &lt;tjluc@nacimiento.com&gt;
 * @since   lucene 1.3
 * @version 0.1
 * @see IndexSearcher
 */
public class IntegerSortedSearcher
extends IndexSearcher {

    /** stores the field being used to sort by **/
    protected String field;

    /**
     * Searches the index in the named directory using the given
     * field as the primary sort.
     * The terms in the field must contain strictly integers in
     * the range <code>Integer.MIN_VALUE</code> and <code>Integer.MAX_VALUE</code> inclusive.
     * @see IndexSearcher(java.lang.String,java.lang.String)
     */
    public IntegerSortedSearcher(String path, String integer_field)
    throws IOException {
        this(IndexReader.open(path), integer_field);
    }

    /**
     * Searches the index in the provided directory using the
     * given field as the primary sort.
     * The terms in the field must contain strictly integers in
     * the range <code>Integer.MIN_VALUE</code> and <code>Integer.MAX_VALUE</code> inclusive.
     * @see IndexSearcher(Directory,java.lang.String)
     */
    public IntegerSortedSearcher(Directory directory, String integer_field)
    throws IOException {
        this(IndexReader.open(directory), integer_field);
    }

    /**
     * Searches the provided index using the given field as the
     * primary sort.
     * The terms in the field must contain strictly integers in
     * the range <code>Integer.MIN_VALUE</code> and <code>Integer.MAX_VALUE</code> inclusive.
     * @see IndexSearcher(IndexReader)
     */
    public IntegerSortedSearcher(IndexReader r, String integer_field) {
        super(r);
        this.field = integer_field.intern();
    }

    /**
     * Sets the field to order results by.  This can be called
     * multiple times per instance of IntegerSortedSearcher.
     * @param integer_field  The field to sort results by.
     */
    public void setOrderByField(String integer_field) {
        this.field = integer_field.intern();
    }

    /**
     * Returns the name of the field currently being used
     * to sort results by.
     * @return  Field name.
     */
    public String getOrderByField() {
        return field;
    }


    /**
     * Finds the top <code>nDocs</code>
     * hits for <code>query</code>, applying <code>filter</code> if non-null.
     *
     * Overrides IndexSearcher.search to use a FieldSortedHitQueue instead of the
     * default HitQueue.
     *
     * @see IndexSearcher#search
     */
    public TopDocs search(Query query, Filter filter, final int nDocs)
    throws IOException {

        Scorer scorer = query.weight(this).scorer(reader);
        if (scorer == null) {
            return new TopDocs(0, new ScoreDoc[0]);
        }

        final BitSet bits = filter != null ? filter.bits(reader) : null;
        final FieldSortedHitQueue hq = new FieldSortedHitQueue(reader, field, nDocs);
        final int[] totalHits = new int[1];
        scorer.score(
            new HitCollector() {
                public final void collect(int doc, float score) {
                    if (score > 0.0f &&                         // ignore zeroed buckets
                        (bits == null || bits.get(doc))) {      // skip docs not in bits
                        totalHits[0]++;
                        hq.insert(new ScoreDoc(doc, score));
                    }
                }
            });

        ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
        for (int i = hq.size() - 1; i >= 0; i--) {              // put docs in array
            scoreDocs[i] = (ScoreDoc) hq.pop();
        }

        return new TopDocs(totalHits[0], scoreDocs);
    }
}
