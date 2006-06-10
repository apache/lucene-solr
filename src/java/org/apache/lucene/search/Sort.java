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

import java.io.Serializable;


/**
 * Encapsulates sort criteria for returned hits.
 *
 * <p>The fields used to determine sort order must be carefully chosen.
 * Documents must contain a single term in such a field,
 * and the value of the term should indicate the document's relative position in
 * a given sort order.  The field must be indexed, but should not be tokenized,
 * and does not need to be stored (unless you happen to want it back with the
 * rest of your document data).  In other words:
 *
 * <p><code>document.add (new Fieldable ("byNumber", Integer.toString(x), Fieldable.Store.NO, Fieldable.Index.UN_TOKENIZED));</code></p>
 * 
 *
 * <p><h3>Valid Types of Values</h3>
 *
 * <p>There are three possible kinds of term values which may be put into
 * sorting fields: Integers, Floats, or Strings.  Unless
 * {@link SortField SortField} objects are specified, the type of value
 * in the field is determined by parsing the first term in the field.
 *
 * <p>Integer term values should contain only digits and an optional
 * preceeding negative sign.  Values must be base 10 and in the range
 * <code>Integer.MIN_VALUE</code> and <code>Integer.MAX_VALUE</code> inclusive.
 * Documents which should appear first in the sort
 * should have low value integers, later documents high values
 * (i.e. the documents should be numbered <code>1..n</code> where
 * <code>1</code> is the first and <code>n</code> the last).
 *
 * <p>Float term values should conform to values accepted by
 * {@link Float Float.valueOf(String)} (except that <code>NaN</code>
 * and <code>Infinity</code> are not supported).
 * Documents which should appear first in the sort
 * should have low values, later documents high values.
 *
 * <p>String term values can contain any valid String, but should
 * not be tokenized.  The values are sorted according to their
 * {@link Comparable natural order}.  Note that using this type
 * of term value has higher memory requirements than the other
 * two types.
 *
 * <p><h3>Object Reuse</h3>
 *
 * <p>One of these objects can be
 * used multiple times and the sort order changed between usages.
 *
 * <p>This class is thread safe.
 *
 * <p><h3>Memory Usage</h3>
 *
 * <p>Sorting uses of caches of term values maintained by the
 * internal HitQueue(s).  The cache is static and contains an integer
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
 * <p>Created: Feb 12, 2004 10:53:57 AM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
public class Sort
implements Serializable {

  /**
   * Represents sorting by computed relevance. Using this sort criteria returns
   * the same results as calling
   * {@link Searcher#search(Query) Searcher#search()}without a sort criteria,
   * only with slightly more overhead.
   */
  public static final Sort RELEVANCE = new Sort();

  /** Represents sorting by index order. */
  public static final Sort INDEXORDER = new Sort(SortField.FIELD_DOC);

  // internal representation of the sort criteria
  SortField[] fields;

  /**
   * Sorts by computed relevance. This is the same sort criteria as calling
   * {@link Searcher#search(Query) Searcher#search()}without a sort criteria,
   * only with slightly more overhead.
   */
  public Sort() {
    this(new SortField[] { SortField.FIELD_SCORE, SortField.FIELD_DOC });
  }

  /**
   * Sorts by the terms in <code>field</code> then by index order (document
   * number). The type of value in <code>field</code> is determined
   * automatically.
   * 
   * @see SortField#AUTO
   */
  public Sort(String field) {
    setSort(field, false);
  }

  /**
   * Sorts possibly in reverse by the terms in <code>field</code> then by
   * index order (document number). The type of value in <code>field</code> is
   * determined automatically.
   * 
   * @see SortField#AUTO
   */
  public Sort(String field, boolean reverse) {
    setSort(field, reverse);
  }

  /**
   * Sorts in succession by the terms in each field. The type of value in
   * <code>field</code> is determined automatically.
   * 
   * @see SortField#AUTO
   */
  public Sort(String[] fields) {
    setSort(fields);
  }

  /** Sorts by the criteria in the given SortField. */
  public Sort(SortField field) {
    setSort(field);
  }

  /** Sorts in succession by the criteria in each SortField. */
  public Sort(SortField[] fields) {
    setSort(fields);
  }

  /**
   * Sets the sort to the terms in <code>field</code> then by index order
   * (document number).
   */
  public final void setSort(String field) {
    setSort(field, false);
  }

  /**
   * Sets the sort to the terms in <code>field</code> possibly in reverse,
   * then by index order (document number).
   */
  public void setSort(String field, boolean reverse) {
    SortField[] nfields = new SortField[] {
        new SortField(field, SortField.AUTO, reverse), SortField.FIELD_DOC };
    fields = nfields;
  }

  /** Sets the sort to the terms in each field in succession. */
  public void setSort(String[] fieldnames) {
    final int n = fieldnames.length;
    SortField[] nfields = new SortField[n];
    for (int i = 0; i < n; ++i) {
      nfields[i] = new SortField(fieldnames[i], SortField.AUTO);
    }
    fields = nfields;
  }

  /** Sets the sort to the given criteria. */
  public void setSort(SortField field) {
    this.fields = new SortField[] { field };
  }

  /** Sets the sort to the given criteria in succession. */
  public void setSort(SortField[] fields) {
    this.fields = fields;
  }
  
  /**
   * Representation of the sort criteria.
   * @return Array of SortField objects used in this sort criteria
   */
  public SortField[] getSort() {
    return fields;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();

    for (int i = 0; i < fields.length; i++) {
      buffer.append(fields[i].toString());
      if ((i+1) < fields.length)
        buffer.append(',');
    }

    return buffer.toString();
  }
}
