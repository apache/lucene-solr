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
import java.util.Comparator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

// TODO(simonw) -- for cleaner transition, maybe we should make
// a new SortField that subclasses this one and always uses
// index values?

/**
 * Stores information about how to sort documents by terms in an individual
 * field.  Fields must be indexed in order to sort by them.
 *
 * <p>Created: Feb 11, 2004 1:25:29 PM
 *
 * @since   lucene 1.4
 * @see Sort
 */
public class SortField {

  public static enum Type {

    /** Sort by document score (relevance).  Sort values are Float and higher
     * values are at the front. */
    SCORE,

    /** Sort by document number (index order).  Sort values are Integer and lower
     * values are at the front. */
    DOC,

    /** Sort using term values as Strings.  Sort values are String and lower
     * values are at the front. */
    STRING,

    /** Sort using term values as encoded Integers.  Sort values are Integer and
     * lower values are at the front. */
    INT,

    /** Sort using term values as encoded Floats.  Sort values are Float and
     * lower values are at the front. */
    FLOAT,

    /** Sort using term values as encoded Longs.  Sort values are Long and
     * lower values are at the front. */
    LONG,

    /** Sort using term values as encoded Doubles.  Sort values are Double and
     * lower values are at the front. */
    DOUBLE,

    /** Sort using term values as encoded Shorts.  Sort values are Short and
     * lower values are at the front. */
    SHORT,

    /** Sort using a custom Comparator.  Sort values are any Comparable and
     * sorting is done according to natural order. */
    CUSTOM,

    /** Sort using term values as encoded Bytes.  Sort values are Byte and
     * lower values are at the front. */
    BYTE,

    /** Sort using term values as Strings, but comparing by
     * value (using String.compareTo) for all comparisons.
     * This is typically slower than {@link #STRING}, which
     * uses ordinals to do the sorting. */
    STRING_VAL,

    /** Sort use byte[] index values. */
    BYTES,

    /** Force rewriting of SortField using {@link SortField#rewrite(IndexSearcher)}
     * before it can be used for sorting */
    REWRITEABLE
  }

  /** Represents sorting by document score (relevance). */
  public static final SortField FIELD_SCORE = new SortField(null, Type.SCORE);

  /** Represents sorting by document number (index order). */
  public static final SortField FIELD_DOC = new SortField(null, Type.DOC);

  private String field;
  private Type type;  // defaults to determining type dynamically
  boolean reverse = false;  // defaults to natural order
  private FieldCache.Parser parser;

  // Used for CUSTOM sort
  private FieldComparatorSource comparatorSource;

  // Used for 'sortMissingFirst/Last'
  public Object missingValue = null;

  /** Creates a sort by terms in the given field with the type of term
   * values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   */
  public SortField(String field, Type type) {
    initFieldType(field, type);
  }

  /** Creates a sort, possibly in reverse, by terms in the given field with the
   * type of term values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   * @param reverse True if natural order should be reversed.
   */
  public SortField(String field, Type type, boolean reverse) {
    initFieldType(field, type);
    this.reverse = reverse;
  }

  /** Creates a sort by terms in the given field, parsed
   * to numeric values using a custom {@link FieldCache.Parser}.
   * @param field  Name of field to sort by.  Must not be null.
   * @param parser Instance of a {@link FieldCache.Parser},
   *  which must subclass one of the existing numeric
   *  parsers from {@link FieldCache}. Sort type is inferred
   *  by testing which numeric parser the parser subclasses.
   * @throws IllegalArgumentException if the parser fails to
   *  subclass an existing numeric parser, or field is null
   */
  public SortField(String field, FieldCache.Parser parser) {
    this(field, parser, false);
  }

  /** Creates a sort, possibly in reverse, by terms in the given field, parsed
   * to numeric values using a custom {@link FieldCache.Parser}.
   * @param field  Name of field to sort by.  Must not be null.
   * @param parser Instance of a {@link FieldCache.Parser},
   *  which must subclass one of the existing numeric
   *  parsers from {@link FieldCache}. Sort type is inferred
   *  by testing which numeric parser the parser subclasses.
   * @param reverse True if natural order should be reversed.
   * @throws IllegalArgumentException if the parser fails to
   *  subclass an existing numeric parser, or field is null
   */
  public SortField(String field, FieldCache.Parser parser, boolean reverse) {
    if (parser instanceof FieldCache.IntParser) initFieldType(field, Type.INT);
    else if (parser instanceof FieldCache.FloatParser) initFieldType(field, Type.FLOAT);
    else if (parser instanceof FieldCache.ShortParser) initFieldType(field, Type.SHORT);
    else if (parser instanceof FieldCache.ByteParser) initFieldType(field, Type.BYTE);
    else if (parser instanceof FieldCache.LongParser) initFieldType(field, Type.LONG);
    else if (parser instanceof FieldCache.DoubleParser) initFieldType(field, Type.DOUBLE);
    else {
      throw new IllegalArgumentException("Parser instance does not subclass existing numeric parser from FieldCache (got " + parser + ")");
    }

    this.reverse = reverse;
    this.parser = parser;
  }
  
  public SortField setMissingValue(Object missingValue) {
    if (type != Type.BYTE && type != Type.SHORT && type != Type.INT && type != Type.FLOAT && type != Type.LONG && type != Type.DOUBLE) {
      throw new IllegalArgumentException( "Missing value only works for numeric types" );
    }
    this.missingValue = missingValue;
    return this;
  }

  /** Creates a sort with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   */
  public SortField(String field, FieldComparatorSource comparator) {
    initFieldType(field, Type.CUSTOM);
    this.comparatorSource = comparator;
  }

  /** Creates a sort, possibly in reverse, with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   * @param reverse True if natural order should be reversed.
   */
  public SortField(String field, FieldComparatorSource comparator, boolean reverse) {
    initFieldType(field, Type.CUSTOM);
    this.reverse = reverse;
    this.comparatorSource = comparator;
  }

  // Sets field & type, and ensures field is not NULL unless
  // type is SCORE or DOC
  private void initFieldType(String field, Type type) {
    this.type = type;
    if (field == null) {
      if (type != Type.SCORE && type != Type.DOC) {
        throw new IllegalArgumentException("field can only be null when type is SCORE or DOC");
      }
    } else {
      this.field = field;
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
   * @return One of the constants SCORE, DOC, STRING, INT or FLOAT.
   */
  public Type getType() {
    return type;
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

  /** Returns the {@link FieldComparatorSource} used for
   * custom sorting
   */
  public FieldComparatorSource getComparatorSource() {
    return comparatorSource;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    String dv = useIndexValues ? " [dv]" : "";
    switch (type) {
      case SCORE:
        buffer.append("<score>");
        break;

      case DOC:
        buffer.append("<doc>");
        break;

      case STRING:
        buffer.append("<string" + dv + ": \"").append(field).append("\">");
        break;

      case STRING_VAL:
        buffer.append("<string_val" + dv + ": \"").append(field).append("\">");
        break;

      case BYTE:
        buffer.append("<byte: \"").append(field).append("\">");
        break;

      case SHORT:
        buffer.append("<short: \"").append(field).append("\">");
        break;

      case INT:
        buffer.append("<int" + dv + ": \"").append(field).append("\">");
        break;

      case LONG:
        buffer.append("<long: \"").append(field).append("\">");
        break;

      case FLOAT:
        buffer.append("<float" + dv + ": \"").append(field).append("\">");
        break;

      case DOUBLE:
        buffer.append("<double" + dv + ": \"").append(field).append("\">");
        break;

      case CUSTOM:
        buffer.append("<custom:\"").append(field).append("\": ").append(comparatorSource).append('>');
        break;
      
      case REWRITEABLE:
        buffer.append("<rewriteable: \"").append(field).append("\">");
        break;

      default:
        buffer.append("<???: \"").append(field).append("\">");
        break;
    }

    if (reverse) buffer.append('!');

    return buffer.toString();
  }

  /** Returns true if <code>o</code> is equal to this.  If a
   *  {@link FieldComparatorSource} or {@link
   *  FieldCache.Parser} was provided, it must properly
   *  implement equals (unless a singleton is always used). */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SortField)) return false;
    final SortField other = (SortField)o;
    return (
      StringHelper.equals(other.field, this.field)
      && other.type == this.type
      && other.reverse == this.reverse
      && (other.comparatorSource == null ? this.comparatorSource == null : other.comparatorSource.equals(this.comparatorSource))
    );
  }

  /** Returns true if <code>o</code> is equal to this.  If a
   *  {@link FieldComparatorSource} or {@link
   *  FieldCache.Parser} was provided, it must properly
   *  implement hashCode (unless a singleton is always
   *  used). */
  @Override
  public int hashCode() {
    int hash = type.hashCode() ^ 0x346565dd + Boolean.valueOf(reverse).hashCode() ^ 0xaf5998bb;
    if (field != null) hash += field.hashCode()^0xff5685dd;
    if (comparatorSource != null) hash += comparatorSource.hashCode();
    return hash;
  }

  private boolean useIndexValues;

  public void setUseIndexValues(boolean b) {
    useIndexValues = b;
  }

  public boolean getUseIndexValues() {
    return useIndexValues;
  }

  private Comparator<BytesRef> bytesComparator = BytesRef.getUTF8SortedAsUnicodeComparator();

  public void setBytesComparator(Comparator<BytesRef> b) {
    bytesComparator = b;
  }

  public Comparator<BytesRef> getBytesComparator() {
    return bytesComparator;
  }

  /** Returns the {@link FieldComparator} to use for
   * sorting.
   *
   * @lucene.experimental
   *
   * @param numHits number of top hits the queue will store
   * @param sortPos position of this SortField within {@link
   *   Sort}.  The comparator is primary if sortPos==0,
   *   secondary if sortPos==1, etc.  Some comparators can
   *   optimize themselves when they are the primary sort.
   * @return {@link FieldComparator} to use when sorting
   */
  public FieldComparator<?> getComparator(final int numHits, final int sortPos) throws IOException {

    switch (type) {
    case SCORE:
      return new FieldComparator.RelevanceComparator(numHits);

    case DOC:
      return new FieldComparator.DocComparator(numHits);

    case INT:
      if (useIndexValues) {
        return new FieldComparator.IntDocValuesComparator(numHits, field);
      } else {
        return new FieldComparator.IntComparator(numHits, field, parser, (Integer) missingValue);
      }

    case FLOAT:
      if (useIndexValues) {
        return new FieldComparator.FloatDocValuesComparator(numHits, field);
      } else {
        return new FieldComparator.FloatComparator(numHits, field, parser, (Float) missingValue);
      }

    case LONG:
      return new FieldComparator.LongComparator(numHits, field, parser, (Long) missingValue);

    case DOUBLE:
      return new FieldComparator.DoubleComparator(numHits, field, parser, (Double) missingValue);

    case BYTE:
      return new FieldComparator.ByteComparator(numHits, field, parser, (Byte) missingValue);

    case SHORT:
      return new FieldComparator.ShortComparator(numHits, field, parser, (Short) missingValue);

    case CUSTOM:
      assert comparatorSource != null;
      return comparatorSource.newComparator(field, numHits, sortPos, reverse);

    case STRING:
      if (useIndexValues) {
        return new FieldComparator.TermOrdValDocValuesComparator(numHits, field);
      } else {
        return new FieldComparator.TermOrdValComparator(numHits, field);
      }

    case STRING_VAL:
      if (useIndexValues) {
        return new FieldComparator.TermValDocValuesComparator(numHits, field);
      } else {
        return new FieldComparator.TermValComparator(numHits, field);
      }

    case REWRITEABLE:
      throw new IllegalStateException("SortField needs to be rewritten through Sort.rewrite(..) and SortField.rewrite(..)");
        
    default:
      throw new IllegalStateException("Illegal sort type: " + type);
    }
  }

  /**
   * Rewrites this SortField, returning a new SortField if a change is made.
   * Subclasses should override this define their rewriting behavior when this
   * SortField is of type {@link SortField.Type#REWRITEABLE}
   *
   * @param searcher IndexSearcher to use during rewriting
   * @return New rewritten SortField, or {@code this} if nothing has changed.
   * @throws IOException Can be thrown by the rewriting
   * @lucene.experimental
   */
  public SortField rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }
}
