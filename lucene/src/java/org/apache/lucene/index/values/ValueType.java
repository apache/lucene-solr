package org.apache.lucene.index.values;

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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.values.IndexDocValues.SortedSource;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * <code>ValueType</code> specifies the {@link IndexDocValues} type for a
 * certain field. A <code>ValueType</code> only defines the data type for a field
 * while the actual implementation used to encode and decode the values depends
 * on the the {@link Codec#docsConsumer} and {@link Codec#docsProducer} methods.
 * 
 * @lucene.experimental
 */
public enum ValueType {

  /**
   * A variable bit signed integer value. By default this type uses
   * {@link PackedInts} to compress the values, as an offset
   * from the minimum value, as long as the value range
   * fits into 2<sup>63</sup>-1. Otherwise,
   * the default implementation falls back to fixed size 64bit
   * integers ({@link #FIXED_INTS_64}).
   * <p>
   * NOTE: this type uses <tt>0</tt> as the default value without any
   * distinction between provided <tt>0</tt> values during indexing. All
   * documents without an explicit value will use <tt>0</tt> instead. In turn,
   * {@link ValuesEnum} instances will not skip documents without an explicit
   * value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  VAR_INTS,
  
  /**
   * A 8 bit signed integer value. {@link Source} instances of
   * this type return a <tt>byte</tt> array from {@link Source#getArray()}
   * <p>
   * NOTE: this type uses <tt>0</tt> as the default value without any
   * distinction between provided <tt>0</tt> values during indexing. All
   * documents without an explicit value will use <tt>0</tt> instead. In turn,
   * {@link ValuesEnum} instances will not skip documents without an explicit
   * value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  FIXED_INTS_8,
  
  /**
   * A 16 bit signed integer value. {@link Source} instances of
   * this type return a <tt>short</tt> array from {@link Source#getArray()}
   * <p>
   * NOTE: this type uses <tt>0</tt> as the default value without any
   * distinction between provided <tt>0</tt> values during indexing. All
   * documents without an explicit value will use <tt>0</tt> instead. In turn,
   * {@link ValuesEnum} instances will not skip documents without an explicit
   * value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  FIXED_INTS_16,
  
  /**
   * A 32 bit signed integer value. {@link Source} instances of
   * this type return a <tt>int</tt> array from {@link Source#getArray()}
   * <p>
   * NOTE: this type uses <tt>0</tt> as the default value without any
   * distinction between provided <tt>0</tt> values during indexing. All
   * documents without an explicit value will use <tt>0</tt> instead. In turn,
   * {@link ValuesEnum} instances will not skip documents without an explicit
   * value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  FIXED_INTS_32,

  /**
   * A 64 bit signed integer value. {@link Source} instances of
   * this type return a <tt>long</tt> array from {@link Source#getArray()}
   * <p>
   * NOTE: this type uses <tt>0</tt> as the default value without any
   * distinction between provided <tt>0</tt> values during indexing. All
   * documents without an explicit value will use <tt>0</tt> instead. In turn,
   * {@link ValuesEnum} instances will not skip documents without an explicit
   * value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  FIXED_INTS_64,
  /**
   * A 32 bit floating point value. By default there is no compression
   * applied. To fit custom float values into less than 32bit either a custom
   * implementation is needed or values must be encoded into a
   * {@link #BYTES_FIXED_STRAIGHT} type. {@link Source} instances of
   * this type return a <tt>float</tt> array from {@link Source#getArray()}
   * <p>
   * NOTE: this type uses <tt>0.0f</tt> as the default value without any
   * distinction between provided <tt>0.0f</tt> values during indexing. All
   * documents without an explicit value will use <tt>0.0f</tt> instead. In
   * turn, {@link ValuesEnum} instances will not skip documents without an
   * explicit value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  FLOAT_32,
  /**
   * 
   * A 64 bit floating point value. By default there is no compression
   * applied. To fit custom float values into less than 64bit either a custom
   * implementation is needed or values must be encoded into a
   * {@link #BYTES_FIXED_STRAIGHT} type. {@link Source} instances of
   * this type return a <tt>double</tt> array from {@link Source#getArray()}
   * <p>
   * NOTE: this type uses <tt>0.0d</tt> as the default value without any
   * distinction between provided <tt>0.0d</tt> values during indexing. All
   * documents without an explicit value will use <tt>0.0d</tt> instead. In
   * turn, {@link ValuesEnum} instances will not skip documents without an
   * explicit value assigned. Custom default values must be assigned explicitly.
   * </p>
   */
  FLOAT_64,

  // TODO(simonw): -- shouldn't lucene decide/detect straight vs
  // deref, as well fixed vs var?
  /**
   * A fixed length straight byte[]. All values added to
   * such a field must be of the same length. All bytes are stored sequentially
   * for fast offset access.
   * <p>
   * NOTE: this type uses <tt>0 byte</tt> filled byte[] based on the length of the first seen
   * value as the default value without any distinction between explicitly
   * provided values during indexing. All documents without an explicit value
   * will use the default instead. In turn, {@link ValuesEnum} instances will
   * not skip documents without an explicit value assigned. Custom default
   * values must be assigned explicitly.
   * </p>
   */
  BYTES_FIXED_STRAIGHT,

  /**
   * A fixed length dereferenced byte[] variant. Fields with
   * this type only store distinct byte values and store an additional offset
   * pointer per document to dereference the shared byte[].
   * Use this type if your documents may share the same byte[].
   * <p>
   * NOTE: Fields of this type will not store values for documents without and
   * explicitly provided value. If a documents value is accessed while no
   * explicit value is stored the returned {@link BytesRef} will be a 0-length
   * reference. In turn, {@link ValuesEnum} instances will skip over documents
   * without an explicit value assigned. Custom default values must be assigned
   * explicitly.
   * </p>
   */
  BYTES_FIXED_DEREF,

  /**
   * A fixed length pre-sorted byte[] variant. Fields with this type only
   * store distinct byte values and store an additional offset pointer per
   * document to dereference the shared byte[]. The stored
   * byte[] is presorted, by default by unsigned byte order,
   * and allows access via document id, ordinal and by-value.
   * Use this type if your documents may share the same byte[].
   * <p>
   * NOTE: Fields of this type will not store values for documents without and
   * explicitly provided value. If a documents value is accessed while no
   * explicit value is stored the returned {@link BytesRef} will be a 0-length
   * reference. In turn, {@link ValuesEnum} instances will skip over documents
   * without an explicit value assigned. Custom default values must be assigned
   * explicitly.
   * </p>
   * 
   * @see SortedSource
   */
  BYTES_FIXED_SORTED,

  /**
   * Variable length straight stored byte[] variant. All bytes are
   * stored sequentially for compactness. Usage of this type via the
   * disk-resident API might yield performance degradation since no additional
   * index is used to advance by more than one document value at a time.
   * <p>
   * NOTE: Fields of this type will not store values for documents without an
   * explicitly provided value. If a documents value is accessed while no
   * explicit value is stored the returned {@link BytesRef} will be a 0-length
   * byte[] reference.  In contrast to dereferenced variants, {@link ValuesEnum}
   * instances will <b>not</b> skip over documents without an explicit value
   * assigned.  Custom default values must be assigned explicitly.
   * </p>
   */
  BYTES_VAR_STRAIGHT,

  /**
   * A variable length dereferenced byte[]. Just like
   * {@link #BYTES_FIXED_DEREF}, but allowing each
   * document's value to be a different length.
   * <p>
   * NOTE: Fields of this type will not store values for documents without and
   * explicitly provided value. If a documents value is accessed while no
   * explicit value is stored the returned {@link BytesRef} will be a 0-length
   * reference. In turn, {@link ValuesEnum} instances will skip over documents
   * without an explicit value assigned. Custom default values must be assigned
   * explicitly.
   * </p>
   */
  BYTES_VAR_DEREF,

  /**
   * A variable length pre-sorted byte[] variant. Just like
   * {@link #BYTES_FIXED_SORTED}, but allowing each
   * document's value to be a different length.
   * <p>
   * NOTE: Fields of this type will not store values for documents without and
   * explicitly provided value. If a documents value is accessed while no
   * explicit value is stored the returned {@link BytesRef} will be a 0-length
   * reference. In turn, {@link ValuesEnum} instances will skip over documents
   * without an explicit value assigned. Custom default values must be assigned
   * explicitly.
   * </p>
   * 
   * @see SortedSource
   */
  BYTES_VAR_SORTED
}
