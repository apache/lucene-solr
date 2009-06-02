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

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

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
   * string characters.
   * @deprecated Please specify the exact type, instead.*/
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

  /** Sort using term values as encoded Shorts.  Sort values are Short and
   * lower values are at the front. */
  public static final int SHORT = 8;

  /** Sort using a custom Comparator.  Sort values are any Comparable and
   * sorting is done according to natural order. */
  public static final int CUSTOM = 9;

  /** Sort using term values as encoded Bytes.  Sort values are Byte and
   * lower values are at the front. */
  public static final int BYTE = 10;
  
  /** Sort using term values as Strings, but comparing by
   * value (using String.compareTo) for all comparisons.
   * This is typically slower than {@link #STRING}, which
   * uses ordinals to do the sorting. */
  public static final int STRING_VAL = 11;
  
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
  private FieldCache.Parser parser;

  // Used for CUSTOM sort
  private FieldComparatorSource comparatorSource;

  private boolean useLegacy = false; // remove in Lucene 3.0

  /** Creates a sort by terms in the given field where the type of term value
   * is determined dynamically ({@link #AUTO AUTO}).
   * @param field Name of field to sort by, cannot be
   * <code>null</code>.
   * @deprecated Please specify the exact type instead.
   */
  public SortField (String field) {
    initFieldType(field, AUTO);
  }

  /** Creates a sort, possibly in reverse, by terms in the given field where
   * the type of term value is determined dynamically ({@link #AUTO AUTO}).
   * @param field Name of field to sort by, cannot be <code>null</code>.
   * @param reverse True if natural order should be reversed.
   * @deprecated Please specify the exact type instead.
   */
  public SortField (String field, boolean reverse) {
    initFieldType(field, AUTO);
    this.reverse = reverse;
  }

  /** Creates a sort by terms in the given field with the type of term
   * values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   */
  public SortField (String field, int type) {
    initFieldType(field, type);
  }

  /** Creates a sort, possibly in reverse, by terms in the given field with the
   * type of term values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   * @param reverse True if natural order should be reversed.
   */
  public SortField (String field, int type, boolean reverse) {
    initFieldType(field, type);
    this.reverse = reverse;
  }

  /** Creates a sort by terms in the given field, parsed
   * to numeric values using a custom {@link FieldCache.Parser}.
   * @param field  Name of field to sort by.  Must not be null.
   * @param parser Instance of a {@link FieldCache.Parser},
   *  which must subclass one of the existing numeric
   *  parsers from {@link FieldCache} or {@link
   *  ExtendedFieldCache}. Sort type is inferred by testing
   *  which numeric parser the parser subclasses.
   * @throws IllegalArgumentException if the parser fails to
   *  subclass an existing numeric parser, or field is null
   */
  public SortField (String field, FieldCache.Parser parser) {
    this(field, parser, false);
  }

  /** Creates a sort, possibly in reverse, by terms in the given field, parsed
   * to numeric values using a custom {@link FieldCache.Parser}.
   * @param field  Name of field to sort by.  Must not be null.
   * @param parser Instance of a {@link FieldCache.Parser},
   *  which must subclass one of the existing numeric
   *  parsers from {@link FieldCache} or {@link
   *  ExtendedFieldCache}. Sort type is inferred by testing
   *  which numeric parser the parser subclasses.
   * @param reverse True if natural order should be reversed.
   * @throws IllegalArgumentException if the parser fails to
   *  subclass an existing numeric parser, or field is null
   */
  public SortField (String field, FieldCache.Parser parser, boolean reverse) {
    if (parser instanceof FieldCache.IntParser) initFieldType(field, INT);
    else if (parser instanceof FieldCache.FloatParser) initFieldType(field, FLOAT);
    else if (parser instanceof FieldCache.ShortParser) initFieldType(field, SHORT);
    else if (parser instanceof FieldCache.ByteParser) initFieldType(field, BYTE);
    else if (parser instanceof ExtendedFieldCache.LongParser) initFieldType(field, LONG);
    else if (parser instanceof ExtendedFieldCache.DoubleParser) initFieldType(field, DOUBLE);
    else
      throw new IllegalArgumentException("Parser instance does not subclass existing numeric parser from FieldCache or ExtendedFieldCache (got " + parser + ")");

    this.reverse = reverse;
    this.parser = parser;
  }

  /** Creates a sort by terms in the given field sorted
   * according to the given locale.
   * @param field  Name of field to sort by, cannot be <code>null</code>.
   * @param locale Locale of values in the field.
   */
  public SortField (String field, Locale locale) {
    initFieldType(field, STRING);
    this.locale = locale;
  }

  /** Creates a sort, possibly in reverse, by terms in the given field sorted
   * according to the given locale.
   * @param field  Name of field to sort by, cannot be <code>null</code>.
   * @param locale Locale of values in the field.
   */
  public SortField (String field, Locale locale, boolean reverse) {
    initFieldType(field, STRING);
    this.locale = locale;
    this.reverse = reverse;
  }

  /** Creates a sort with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   * @deprecated use SortField (String field, FieldComparatorSource comparator)
   */
  public SortField (String field, SortComparatorSource comparator) {
    initFieldType(field, CUSTOM);
    setUseLegacySearch(true);
    this.factory = comparator;
  }
  
  /** Creates a sort with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   */
  public SortField (String field, FieldComparatorSource comparator) {
    initFieldType(field, CUSTOM);
    this.comparatorSource = comparator;
  }

  /** Creates a sort, possibly in reverse, with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   * @param reverse True if natural order should be reversed.
   * @deprecated use SortField (String field, FieldComparatorSource comparator, boolean reverse)
   */
  public SortField (String field, SortComparatorSource comparator, boolean reverse) {
    initFieldType(field, CUSTOM);
    setUseLegacySearch(true);
    this.reverse = reverse;
    this.factory = comparator;
  }
  
  /** Creates a sort, possibly in reverse, with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   * @param reverse True if natural order should be reversed.
   */
  public SortField (String field, FieldComparatorSource comparator, boolean reverse) {
    initFieldType(field, CUSTOM);
    this.reverse = reverse;
    this.comparatorSource = comparator;
  }

  // Sets field & type, and ensures field is not NULL unless
  // type is SCORE or DOC
  private void initFieldType(String field, int type) {
    this.type = type;
    if (field == null) {
      if (type != SCORE && type != DOC)
        throw new IllegalArgumentException("field can only be null when type is SCORE or DOC");
    } else {
      this.field = field.intern();
    }
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

  /** Returns the instance of a {@link FieldCache} parser that fits to the given sort type.
   * May return <code>null</code> if no parser was specified. Sorting is using the default parser then.
   * @return An instance of a {@link FieldCache} parser, or <code>null</code>.
   */
  public FieldCache.Parser getParser() {
    return parser;
  }

  /** Returns whether the sort should be reversed.
   * @return  True if natural order should be reversed.
   */
  public boolean getReverse() {
    return reverse;
  }

  /**
   * @deprecated use {@link #getComparatorSource()}
   */
  public SortComparatorSource getFactory() {
    return factory;
  }
  
  public FieldComparatorSource getComparatorSource() {
    return comparatorSource;
  }
  
  /**
   * Use legacy IndexSearch implementation: search with a MultiSegmentReader rather
   * than passing a single hit collector to multiple SegmentReaders.
   * 
   * @param legacy true for legacy behavior
   * @deprecated will be removed in Lucene 3.0.
   */
  public void setUseLegacySearch(boolean legacy) {
    this.useLegacy = legacy;
  }
  
  /**
   * @return if true, IndexSearch will use legacy sorting search implementation.
   * eg. multiple Priority Queues.
   * @deprecated will be removed in Lucene 3.0.
   */
  public boolean getUseLegacySearch() {
    return this.useLegacy;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    switch (type) {
      case SCORE:
        buffer.append("<score>");
        break;

      case DOC:
        buffer.append("<doc>");
        break;

      case AUTO:
        buffer.append("<auto: \"").append(field).append("\">");
        break;

      case STRING:
        buffer.append("<string: \"").append(field).append("\">");
        break;

      case STRING_VAL:
        buffer.append("<string_val: \"").append(field).append("\">");
        break;

      case BYTE:
        buffer.append("<byte: \"").append(field).append("\">");
        break;

      case SHORT:
        buffer.append("<short: \"").append(field).append("\">");
        break;

      case INT:
        buffer.append("<int: \"").append(field).append("\">");
        break;

      case LONG:
        buffer.append("<long: \"").append(field).append("\">");
        break;

      case FLOAT:
        buffer.append("<float: \"").append(field).append("\">");
        break;

      case DOUBLE:
        buffer.append("<double: \"").append(field).append("\">");
        break;

      case CUSTOM:
        buffer.append("<custom:\"").append(field).append("\": ").append(factory).append('>');
        break;

      default:
        buffer.append("<???: \"").append(field).append("\">");
        break;
    }

    if (locale != null) buffer.append('(').append(locale).append(')');
    if (parser != null) buffer.append('(').append(parser).append(')');
    if (reverse) buffer.append('!');

    return buffer.toString();
  }

  /** Returns true if <code>o</code> is equal to this.  If a
   *  {@link SortComparatorSource} (deprecated) or {@link
   *  FieldCache.Parser} was provided, it must properly
   *  implement equals (unless a singleton is always used). */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SortField)) return false;
    final SortField other = (SortField)o;
    return (
      other.field == this.field // field is always interned
      && other.type == this.type
      && other.reverse == this.reverse
      && (other.locale == null ? this.locale == null : other.locale.equals(this.locale))
      && (other.factory == null ? this.factory == null : other.factory.equals(this.factory))
      && (other.parser == null ? this.parser == null : other.parser.equals(this.parser))
    );
  }

  /** Returns true if <code>o</code> is equal to this.  If a
   *  {@link SortComparatorSource} (deprecated) or {@link
   *  FieldCache.Parser} was provided, it must properly
   *  implement hashCode (unless a singleton is always
   *  used). */
  public int hashCode() {
    int hash=type^0x346565dd + Boolean.valueOf(reverse).hashCode()^0xaf5998bb;
    if (field != null) hash += field.hashCode()^0xff5685dd;
    if (locale != null) hash += locale.hashCode()^0x08150815;
    if (factory != null) hash += factory.hashCode()^0x34987555;
    if (parser != null) hash += parser.hashCode()^0x3aaf56ff;
    return hash;
  }


  /** Returns the {@link FieldComparator} to use for sorting.
   * @param numHits number of top hits the queue will store
   * @param sortPos position of this SortField within {@link
   *   Sort}.  The comparator is primary if sortPos==0,
   *   secondary if sortPos==1, etc.  Some comparators can
   *   optimize themselves when they are the primary sort.
   * @param reversed True if the SortField is reversed
   * @return {@link FieldComparator} to use when sorting
   */
  public FieldComparator getComparator(final int numHits, final int sortPos, final boolean reversed) throws IOException {

    if (locale != null) {
      // TODO: it'd be nice to allow FieldCache.getStringIndex
      // to optionally accept a Locale so sorting could then use
      // the faster StringComparator impls
      return new FieldComparator.StringComparatorLocale(numHits, field, locale);
    }

    switch (type) {
    case SortField.SCORE:
      return new FieldComparator.RelevanceComparator(numHits);

    case SortField.DOC:
      return new FieldComparator.DocComparator(numHits);

    case SortField.INT:
      return new FieldComparator.IntComparator(numHits, field, parser);

    case SortField.FLOAT:
      return new FieldComparator.FloatComparator(numHits, field, parser);

    case SortField.LONG:
      return new FieldComparator.LongComparator(numHits, field, parser);

    case SortField.DOUBLE:
      return new FieldComparator.DoubleComparator(numHits, field, parser);

    case SortField.BYTE:
      return new FieldComparator.ByteComparator(numHits, field, parser);

    case SortField.SHORT:
      return new FieldComparator.ShortComparator(numHits, field, parser);

    case SortField.CUSTOM:
      assert factory == null && comparatorSource != null;
      return comparatorSource.newComparator(field, numHits, sortPos, reversed);

    case SortField.STRING:
      return new FieldComparator.StringOrdValComparator(numHits, field, sortPos, reversed);

    case SortField.STRING_VAL:
      return new FieldComparator.StringValComparator(numHits, field);
        
    default:
      throw new IllegalStateException("Illegal sort type: " + type);
    }
  }
  
  /** Attempts to detect the given field type for an IndexReader. */
  static int detectFieldType(IndexReader reader, String fieldKey) throws IOException {
    String field = fieldKey.intern();
    TermEnum enumerator = reader.terms(new Term(field));
    try {
      Term term = enumerator.term();
      if (term == null) {
        throw new RuntimeException("no terms in field " + field + " - cannot determine sort type");
      }
      int ret = 0;
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
          ret = SortField.INT;
        } catch (NumberFormatException nfe1) {
          try {
            Long.parseLong(termtext);
            ret = SortField.LONG;
          } catch (NumberFormatException nfe2) {
            try {
              Float.parseFloat (termtext);
              ret = SortField.FLOAT;
            } catch (NumberFormatException nfe3) {
              ret = SortField.STRING;
            }
          }
        }         
      } else {
        throw new RuntimeException("field \"" + field + "\" does not appear to be indexed");
      }
      return ret;
    } finally {
      enumerator.close();
    }
  }
}
