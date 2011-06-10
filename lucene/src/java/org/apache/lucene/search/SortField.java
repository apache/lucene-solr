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

import org.apache.lucene.search.cache.*;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.BytesRef;

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

  /** Sort by document score (relevance).  Sort values are Float and higher
   * values are at the front. */
  public static final int SCORE = 0;

  /** Sort by document number (index order).  Sort values are Integer and lower
   * values are at the front. */
  public static final int DOC = 1;

  // reserved, in Lucene 2.9, there was a constant: AUTO = 2;

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
  
  /** Sort use byte[] index values. */
  public static final int BYTES = 12;
  
  /** Represents sorting by document score (relevance). */
  public static final SortField FIELD_SCORE = new SortField (null, SCORE);

  /** Represents sorting by document number (index order). */
  public static final SortField FIELD_DOC = new SortField (null, DOC);

  private String field;
  private int type;  // defaults to determining type dynamically
  boolean reverse = false;  // defaults to natural order
  private CachedArrayCreator<?> creator;
  public Object missingValue = null; // used for 'sortMissingFirst/Last'

  // Used for CUSTOM sort
  private FieldComparatorSource comparatorSource;

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
   *  parsers from {@link FieldCache}. Sort type is inferred
   *  by testing which numeric parser the parser subclasses.
   * @throws IllegalArgumentException if the parser fails to
   *  subclass an existing numeric parser, or field is null
   *  
   *  @deprecated (4.0) use EntryCreator version
   */
  @Deprecated
  public SortField (String field, FieldCache.Parser parser) {
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
   *  
   *  @deprecated (4.0) use EntryCreator version
   */
  @Deprecated
  public SortField (String field, FieldCache.Parser parser, boolean reverse) {
    if (field == null) {
      throw new IllegalArgumentException("field can only be null when type is SCORE or DOC");
    } 
    this.field = StringHelper.intern(field);
    this.reverse = reverse;
    
    if (parser instanceof FieldCache.IntParser) {
      this.type = INT;
      this.creator = new IntValuesCreator( field, (FieldCache.IntParser)parser );
    }
    else if (parser instanceof FieldCache.FloatParser) {
      this.type = FLOAT;
      this.creator = new FloatValuesCreator( field, (FieldCache.FloatParser)parser );
    }
    else if (parser instanceof FieldCache.ShortParser) {
      this.type = SHORT;
      this.creator = new ShortValuesCreator( field, (FieldCache.ShortParser)parser );
    }
    else if (parser instanceof FieldCache.ByteParser) {
      this.type = BYTE;
      this.creator = new ByteValuesCreator( field, (FieldCache.ByteParser)parser );
    }
    else if (parser instanceof FieldCache.LongParser) {
      this.type = LONG;
      this.creator = new LongValuesCreator( field, (FieldCache.LongParser)parser );
    }
    else if (parser instanceof FieldCache.DoubleParser) {
      this.type = DOUBLE;
      this.creator = new DoubleValuesCreator( field, (FieldCache.DoubleParser)parser );
    }
    else
      throw new IllegalArgumentException("Parser instance does not subclass existing numeric parser from FieldCache (got " + parser + ")");

  }
  
  /**
   * Sort by a cached entry value
   * @param creator
   * @param reverse
   */
  public SortField( CachedArrayCreator<?> creator, boolean reverse ) 
  {
    this.field = StringHelper.intern(creator.field);
    this.reverse = reverse;
    this.creator = creator;
    this.type = creator.getSortTypeID();
  }
  
  public SortField setMissingValue( Object v )
  {
    missingValue = v;
    if( missingValue != null ) {
      if( this.creator == null ) {
        throw new IllegalArgumentException( "Missing value only works for sort fields with a CachedArray" );
      }

      // Set the flag to get bits 
      creator.setFlag( CachedArrayCreator.OPTION_CACHE_BITS );
    }
    return this;
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
      this.field = StringHelper.intern(field);
    }
    
    if( creator != null ) {
      throw new IllegalStateException( "creator already exists: "+creator );
    }
    switch( type ) {
    case BYTE:   creator = new ByteValuesCreator( field, null ); break;
    case SHORT:  creator = new ShortValuesCreator( field, null ); break;
    case INT:    creator = new IntValuesCreator( field, null ); break;
    case LONG:   creator = new LongValuesCreator( field, null ); break;
    case FLOAT:  creator = new FloatValuesCreator( field, null ); break;
    case DOUBLE: creator = new DoubleValuesCreator( field, null ); break;
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
  public int getType() {
    return type;
  }

  /** Returns the instance of a {@link FieldCache} parser that fits to the given sort type.
   * May return <code>null</code> if no parser was specified. Sorting is using the default parser then.
   * @return An instance of a {@link FieldCache} parser, or <code>null</code>.
   * @deprecated (4.0) use getEntryCreator()
   */
  @Deprecated
  public FieldCache.Parser getParser() {
    return (creator==null) ? null : creator.getParser();
  }

  public CachedArrayCreator<?> getEntryCreator() {
    return creator;
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
    switch (type) {
      case SCORE:
        buffer.append("<score>");
        break;

      case DOC:
        buffer.append("<doc>");
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
        buffer.append("<custom:\"").append(field).append("\": ").append(comparatorSource).append('>');
        break;

      default:
        buffer.append("<???: \"").append(field).append("\">");
        break;
    }

    if (creator != null) buffer.append('(').append(creator).append(')');
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
      other.field == this.field // field is always interned
      && other.type == this.type
      && other.reverse == this.reverse
      && (other.comparatorSource == null ? this.comparatorSource == null : other.comparatorSource.equals(this.comparatorSource))
      && (other.creator == null ? this.creator == null : other.creator.equals(this.creator))
    );
  }

  /** Returns true if <code>o</code> is equal to this.  If a
   *  {@link FieldComparatorSource} or {@link
   *  FieldCache.Parser} was provided, it must properly
   *  implement hashCode (unless a singleton is always
   *  used). */
  @Override
  public int hashCode() {
    int hash=type^0x346565dd + Boolean.valueOf(reverse).hashCode()^0xaf5998bb;
    if (field != null) hash += field.hashCode()^0xff5685dd;
    if (comparatorSource != null) hash += comparatorSource.hashCode();
    if (creator != null) hash += creator.hashCode()^0x3aaf56ff;
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
  public FieldComparator getComparator(final int numHits, final int sortPos) throws IOException {

    switch (type) {
    case SortField.SCORE:
      return new FieldComparator.RelevanceComparator(numHits);

    case SortField.DOC:
      return new FieldComparator.DocComparator(numHits);

    case SortField.INT:
      if (useIndexValues) {
        return new FieldComparator.IntDocValuesComparator(numHits, field);
      } else {
        return new FieldComparator.IntComparator(numHits, (IntValuesCreator)creator, (Integer) missingValue);
      }

    case SortField.FLOAT:
      if (useIndexValues) {
        return new FieldComparator.FloatDocValuesComparator(numHits, field);
      } else {
        return new FieldComparator.FloatComparator(numHits, (FloatValuesCreator) creator, (Float) missingValue);
      }

    case SortField.LONG:
      return new FieldComparator.LongComparator(numHits, (LongValuesCreator)creator, (Long)missingValue );

    case SortField.DOUBLE:
      return new FieldComparator.DoubleComparator(numHits, (DoubleValuesCreator)creator, (Double)missingValue );

    case SortField.BYTE:
      return new FieldComparator.ByteComparator(numHits, (ByteValuesCreator)creator, (Byte)missingValue );

    case SortField.SHORT:
      return new FieldComparator.ShortComparator(numHits, (ShortValuesCreator)creator, (Short)missingValue );

    case SortField.CUSTOM:
      assert comparatorSource != null;
      return comparatorSource.newComparator(field, numHits, sortPos, reverse);

    case SortField.STRING:
      return new FieldComparator.TermOrdValComparator(numHits, field, sortPos, reverse);

    case SortField.STRING_VAL:
      return new FieldComparator.TermValComparator(numHits, field);
        
    default:
      throw new IllegalStateException("Illegal sort type: " + type);
    }
  }
}
