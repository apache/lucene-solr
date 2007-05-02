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

import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * Abstract base class for sorting hits returned by a Query.
 *
 * <p>This class should only be used if the other SortField
 * types (SCORE, DOC, STRING, INT, FLOAT) do not provide an
 * adequate sorting.  It maintains an internal cache of values which
 * could be quite large.  The cache is an array of Comparable,
 * one for each document in the index.  There is a distinct
 * Comparable for each unique term in the field - if
 * some documents have the same term in the field, the cache
 * array will have entries which reference the same Comparable.
 *
 * <p>Created: Apr 21, 2004 5:08:38 PM
 *
 * @author  Tim Jones
 * @version $Id$
 * @since   1.4
 */
public abstract class SortComparator
implements SortComparatorSource {

  // inherit javadocs
  public ScoreDocComparator newComparator (final IndexReader reader, final String fieldname)
  throws IOException {
    final String field = fieldname.intern();
    final Comparable[] cachedValues = FieldCache.DEFAULT.getCustom (reader, field, SortComparator.this);
    
    return new ScoreDocComparator() {

      public int compare (ScoreDoc i, ScoreDoc j) {
        return cachedValues[i.doc].compareTo (cachedValues[j.doc]);
      }

      public Comparable sortValue (ScoreDoc i) {
        return cachedValues[i.doc];
      }

      public int sortType(){
        return SortField.CUSTOM;
      }
    };
  }

  /**
   * Returns an object which, when sorted according to natural order,
   * will order the Term values in the correct order.
   * <p>For example, if the Terms contained integer values, this method
   * would return <code>new Integer(termtext)</code>.  Note that this
   * might not always be the most efficient implementation - for this
   * particular example, a better implementation might be to make a
   * ScoreDocLookupComparator that uses an internal lookup table of int.
   * @param termtext The textual value of the term.
   * @return An object representing <code>termtext</code> that sorts according to the natural order of <code>termtext</code>.
   * @see Comparable
   * @see ScoreDocComparator
   */
  protected abstract Comparable getComparable (String termtext);

}
