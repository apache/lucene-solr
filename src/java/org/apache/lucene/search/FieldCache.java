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
 * Expert: Maintains caches of term values.
 *
 * <p>Created: May 19, 2004 11:13:14 AM
 *
 * @since   lucene 1.4
 * @version $Id$
 */
public interface FieldCache {

  /** Indicator for StringIndex values in the cache. */
  // NOTE: the value assigned to this constant must not be
  // the same as any of those in SortField!!
  public static final int STRING_INDEX = -1;


  /** Expert: Stores term text values and document ordering data. */
  public static class StringIndex {
	  
    public int binarySearchLookup(String key) {
      // this special case is the reason that Arrays.binarySearch() isn't useful.
      if (key == null)
        return 0;
	  
      int low = 1;
      int high = lookup.length-1;

      while (low <= high) {
        int mid = (low + high) >> 1;
        int cmp = lookup[mid].compareTo(key);

        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid; // key found
      }
      return -(low + 1);  // key not found.
    }
	
    /** All the term values, in natural order. */
    public final String[] lookup;

    /** For each document, an index into the lookup array. */
    public final int[] order;

    /** Creates one of these objects */
    public StringIndex (int[] values, String[] lookup) {
      this.order = values;
      this.lookup = lookup;
    }
  }

  /** Interface to parse bytes from document fields.
   * @see FieldCache#getBytes(IndexReader, String, FieldCache.ByteParser)
   */
  public interface ByteParser {
    /** Return a single Byte representation of this field's value. */
    public byte parseByte(String string);
  }

  /** Interface to parse shorts from document fields.
   * @see FieldCache#getShorts(IndexReader, String, FieldCache.ShortParser)
   */
  public interface ShortParser {
    /** Return a short representation of this field's value. */
    public short parseShort(String string);
  }

  /** Interface to parse ints from document fields.
   * @see FieldCache#getInts(IndexReader, String, FieldCache.IntParser)
   */
  public interface IntParser {
    /** Return an integer representation of this field's value. */
    public int parseInt(String string);
  }

  /** Interface to parse floats from document fields.
   * @see FieldCache#getFloats(IndexReader, String, FieldCache.FloatParser)
   */
  public interface FloatParser {
    /** Return an float representation of this field's value. */
    public float parseFloat(String string);
  }

  /** Expert: The cache used internally by sorting and range query classes. */
  public static FieldCache DEFAULT = new FieldCacheImpl();

  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as a single byte and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the single byte values.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public byte[] getBytes (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as bytes and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the bytes.
   * @param parser  Computes byte for string values.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public byte[] getBytes (IndexReader reader, String field, ByteParser parser)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as shorts and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the shorts.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public short[] getShorts (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as shorts and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the shorts.
   * @param parser  Computes short for string values.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public short[] getShorts (IndexReader reader, String field, ShortParser parser)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as integers and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the integers.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public int[] getInts (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as integers and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the integers.
   * @param parser  Computes integer for string values.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public int[] getInts (IndexReader reader, String field, IntParser parser)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms in <code>field</code> as floats and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the floats.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public float[] getFloats (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms in <code>field</code> as floats and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the floats.
   * @param parser  Computes float for string values.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public float[] getFloats (IndexReader reader, String field,
                            FloatParser parser) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code> and returns an array
   * of size <code>reader.maxDoc()</code> containing the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public String[] getStrings (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none
   * is found reads the term values in <code>field</code> and returns
   * an array of them in natural order, along with an array telling
   * which element in the term array each document uses.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return Array of terms and index into the array for each document.
   * @throws IOException  If any error occurs.
   */
  public StringIndex getStringIndex (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found reads <code>field</code> to see if it contains integers, floats
   * or strings, and then calls one of the other methods in this class to get the
   * values.  For string values, a StringIndex is returned.  After
   * calling this method, there is an entry in the cache for both
   * type <code>AUTO</code> and the actual found type.
   * @param reader  Used to get field values.
   * @param field   Which field contains the values.
   * @return int[], float[] or StringIndex.
   * @throws IOException  If any error occurs.
   */
  public Object getAuto (IndexReader reader, String field)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none
   * is found reads the terms out of <code>field</code> and calls the given SortComparator
   * to get the sort values.  A hit in the cache will happen if <code>reader</code>,
   * <code>field</code>, and <code>comparator</code> are the same (using <code>equals()</code>)
   * as a previous call to this method.
   * @param reader  Used to get field values.
   * @param field   Which field contains the values.
   * @param comparator Used to convert terms into something to sort by.
   * @return Array of sort objects, one for each document.
   * @throws IOException  If any error occurs.
   */
  public Comparable[] getCustom (IndexReader reader, String field, SortComparator comparator)
  throws IOException;
  
}
