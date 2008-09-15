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

import java.io.Serializable;
import java.util.Locale;

/**
 * Stores information about how to sort documents by terms in an individual
 * field.  Fields must be indexed in order to sort by them.
 *
 * <p>Created: Feb 11, 2004 1:25:29 PM
 *
 * @since   lucene 1.4
 * @version $Id$
 * @see Sort
 */
public class SortField
implements Serializable {

  /** Sort by document score (relevancy).  Sort values are Float and higher
   * values are at the front. */
  public static final int SCORE = 0;

  /** Sort by document number (index order).  Sort values are Integer and lower
   * values are at the front. */
  public static final int DOC = 1;

  /** Guess type of sort based on field contents.  A regular expression is used
   * to look at the first term indexed for the field and determine if it
   * represents an integer number, a floating point number, or just arbitrary
   * string characters. */
  public static final int AUTO = 2;

  /** Sort using term values as Strings.  Sort values are String and lower
   * values are at the front. */
  public static final int STRING = 3;

  /** Sort using term values as encoded Integers.  Sort values are Integer and
   * lower values are at the front. */
  public static final int INT = 4;

  /** Sort using term values as encoded Floats.  Sort values are Float and
   * lower values are at the front. */
  public static final int FLOAT = 5;

  /** Sort using term values as encoded Longs.  Sort values are Long and
   * lower values are at the front. */
  public static final int LONG = 6;

  /** Sort using term values as encoded Doubles.  Sort values are Double and
   * lower values are at the front. */
  public static final int DOUBLE = 7;

    /**
   * Sort using term values as encoded Shorts.  Sort values are shorts and lower values are at the front
   */
  public static final int SHORT = 8;


  /** Sort using a custom Comparator.  Sort values are any Comparable and
   * sorting is done according to natural order. */
  public static final int CUSTOM = 9;
  /**
   * Sort using term values as encoded bytes.  Sort values are bytes and lower values are at the front
   */
  public static final int BYTE = 10;


  // IMPLEMENTATION NOTE: the FieldCache.STRING_INDEX is in the same "namespace"
  // as the above static int values.  Any new values must not have the same value
  // as FieldCache.STRING_INDEX.


  /** Represents sorting by document score (relevancy). */
  public static final SortField FIELD_SCORE = new SortField (null, SCORE);

  /** Represents sorting by document number (index order). */
  public static final SortField FIELD_DOC = new SortField (null, DOC);


  private String field;
  private int type = AUTO;  // defaults to determining type dynamically
  private Locale locale;    // defaults to "natural order" (no Locale)
  boolean reverse = false;  // defaults to natural order
  private SortComparatorSource factory;

  /** Creates a sort by terms in the given field where the type of term value
   * is determined dynamically ({@link #AUTO AUTO}).
   * @param field Name of field to sort by, cannot be <code>null</code>.
   */
  public SortField (String field) {
    this.field = field.intern();
  }

  /** Creates a sort, possibly in reverse, by terms in the given field where
   * the type of term value is determined dynamically ({@link #AUTO AUTO}).
   * @param field Name of field to sort by, cannot be <code>null</code>.
   * @param reverse True if natural order should be reversed.
   */
  public SortField (String field, boolean reverse) {
    this.field = field.intern();
    this.reverse = reverse;
  }

  /** Creates a sort by terms in the given field with the type of term
   * values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   */
  public SortField (String field, int type) {
    this.field = (field != null) ? field.intern() : field;
    this.type = type;
  }

  /** Creates a sort, possibly in reverse, by terms in the given field with the
   * type of term values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   * @param reverse True if natural order should be reversed.
   */
  public SortField (String field, int type, boolean reverse) {
    this.field = (field != null) ? field.intern() : field;
    this.type = type;
    this.reverse = reverse;
  }

  /** Creates a sort by terms in the given field sorted
   * according to the given locale.
   * @param field  Name of field to sort by, cannot be <code>null</code>.
   * @param locale Locale of values in the field.
   */
  public SortField (String field, Locale locale) {
    this.field = field.intern();
    this.type = STRING;
    this.locale = locale;
  }

  /** Creates a sort, possibly in reverse, by terms in the given field sorted
   * according to the given locale.
   * @param field  Name of field to sort by, cannot be <code>null</code>.
   * @param locale Locale of values in the field.
   */
  public SortField (String field, Locale locale, boolean reverse) {
    this.field = field.intern();
    this.type = STRING;
    this.locale = locale;
    this.reverse = reverse;
  }

  /** Creates a sort with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   */
  public SortField (String field, SortComparatorSource comparator) {
    this.field = (field != null) ? field.intern() : field;
    this.type = CUSTOM;
    this.factory = comparator;
  }

  /** Creates a sort, possibly in reverse, with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   * @param reverse True if natural order should be reversed.
   */
  public SortField (String field, SortComparatorSource comparator, boolean reverse) {
    this.field = (field != null) ? field.intern() : field;
    this.type = CUSTOM;
    this.reverse = reverse;
    this.factory = comparator;
  }

  /** Returns the name of the field.  Could return <code>null</code>
   * if the sort is by SCORE or DOC.
   * @return Name of field, possibly <code>null</code>.
   */
  public String getField() {
    return field;
  }

  /** Returns the type of contents in the field.
   * @return One of the constants SCORE, DOC, AUTO, STRING, INT or FLOAT.
   */
  public int getType() {
    return type;
  }

  /** Returns the Locale by which term values are interpreted.
   * May return <code>null</code> if no Locale was specified.
   * @return Locale, or <code>null</code>.
   */
  public Locale getLocale() {
    return locale;
  }

  /** Returns whether the sort should be reversed.
   * @return  True if natural order should be reversed.
   */
  public boolean getReverse() {
    return reverse;
  }

  public SortComparatorSource getFactory() {
    return factory;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    switch (type) {
      case SCORE: buffer.append("<score>");
                  break;

      case DOC: buffer.append("<doc>");
                break;

      case CUSTOM:
                buffer.append("<custom:\"").append(field).append("\": ").append(factory).append('>');
                break;

      default:
               buffer.append('\"').append(field).append('\"');
               break;
    }

    if (locale != null) buffer.append('(').append(locale).append(')');
    if (reverse) buffer.append('!');

    return buffer.toString();
  }
}
