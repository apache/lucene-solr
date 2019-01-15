/*
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
package org.apache.lucene.document;


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/**
 * Expert: directly create a field for a document.  Most
 * users should use one of the sugar subclasses: 
 * <ul>
 *    <li>{@link TextField}: {@link Reader} or {@link String} indexed for full-text search
 *    <li>{@link StringField}: {@link String} indexed verbatim as a single token
 *    <li>{@link IntPoint}: {@code int} indexed for exact/range queries.
 *    <li>{@link LongPoint}: {@code long} indexed for exact/range queries.
 *    <li>{@link FloatPoint}: {@code float} indexed for exact/range queries.
 *    <li>{@link DoublePoint}: {@code double} indexed for exact/range queries.
 *    <li>{@link SortedDocValuesField}: {@code byte[]} indexed column-wise for sorting/faceting
 *    <li>{@link SortedSetDocValuesField}: {@code SortedSet<byte[]>} indexed column-wise for sorting/faceting
 *    <li>{@link NumericDocValuesField}: {@code long} indexed column-wise for sorting/faceting
 *    <li>{@link SortedNumericDocValuesField}: {@code SortedSet<long>} indexed column-wise for sorting/faceting
 *    <li>{@link StoredField}: Stored-only value for retrieving in summary results
 * </ul>
 *
 * <p> A field is a section of a Document. Each field has three
 * parts: name, type and value. Values may be text
 * (String, Reader or pre-analyzed TokenStream), binary
 * (byte[]), or numeric (a Number).  Fields are optionally stored in the
 * index, so that they may be returned with hits on the document.
 *
 * <p>
 * NOTE: the field type is an {@link IndexableFieldType}.  Making changes
 * to the state of the IndexableFieldType will impact any
 * Field it is used in.  It is strongly recommended that no
 * changes be made after Field instantiation.
 */
public class Field implements IndexableField {

  /**
   * Field's type
   */
  protected final IndexableFieldType type;

  /**
   * Field's name
   */
  protected final String name;

  /** Field's value */
  protected Object fieldsData;

  /** Pre-analyzed tokenStream for indexed fields; this is
   * separate from fieldsData because you are allowed to
   * have both; eg maybe field has a String value but you
   * customize how it's tokenized */
  protected TokenStream tokenStream;

  /**
   * Expert: creates a field with no initial value.
   * Intended only for custom Field subclasses.
   * @param name field name
   * @param type field type
   * @throws IllegalArgumentException if either the name or type
   *         is null.
   */
  protected Field(String name, IndexableFieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    this.name = name;
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    this.type = type;
  }

  /**
   * Create field with Reader value.
   * @param name field name
   * @param reader reader value
   * @param type field type
   * @throws IllegalArgumentException if either the name or type
   *         is null, or if the field's type is stored(), or
   *         if tokenized() is false.
   * @throws NullPointerException if the reader is null
   */
  public Field(String name, Reader reader, IndexableFieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    if (reader == null) {
      throw new NullPointerException("reader must not be null");
    }
    if (type.stored()) {
      throw new IllegalArgumentException("fields with a Reader value cannot be stored");
    }
    if (type.indexOptions() != IndexOptions.NONE && !type.tokenized()) {
      throw new IllegalArgumentException("non-tokenized fields must use String values");
    }
    
    this.name = name;
    this.fieldsData = reader;
    this.type = type;
  }

  /**
   * Create field with TokenStream value.
   * @param name field name
   * @param tokenStream TokenStream value
   * @param type field type
   * @throws IllegalArgumentException if either the name or type
   *         is null, or if the field's type is stored(), or
   *         if tokenized() is false, or if indexed() is false.
   * @throws NullPointerException if the tokenStream is null
   */
  public Field(String name, TokenStream tokenStream, IndexableFieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    if (tokenStream == null) {
      throw new NullPointerException("tokenStream must not be null");
    }
    if (type.indexOptions() == IndexOptions.NONE || !type.tokenized()) {
      throw new IllegalArgumentException("TokenStream fields must be indexed and tokenized");
    }
    if (type.stored()) {
      throw new IllegalArgumentException("TokenStream fields cannot be stored");
    }
    
    this.name = name;
    this.fieldsData = null;
    this.tokenStream = tokenStream;
    this.type = type;
  }
  
  /**
   * Create field with binary value.
   * 
   * <p>NOTE: the provided byte[] is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param value byte array pointing to binary content (not copied)
   * @param type field type
   * @throws IllegalArgumentException if the field name, value or type
   *         is null, or the field's type is indexed().
   */
  public Field(String name, byte[] value, IndexableFieldType type) {
    this(name, value, 0, value.length, type);
  }

  /**
   * Create field with binary value.
   * 
   * <p>NOTE: the provided byte[] is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param value byte array pointing to binary content (not copied)
   * @param offset starting position of the byte array
   * @param length valid length of the byte array
   * @param type field type
   * @throws IllegalArgumentException if the field name, value or type
   *         is null, or the field's type is indexed().
   */
  public Field(String name, byte[] value, int offset, int length, IndexableFieldType type) {
    this(name, value != null ? new BytesRef(value, offset, length) : null, type);
  }

  /**
   * Create field with binary value.
   *
   * <p>NOTE: the provided BytesRef is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param bytes BytesRef pointing to binary content (not copied)
   * @param type field type
   * @throws IllegalArgumentException if the field name, bytes or type
   *         is null, or the field's type is indexed().
   */
  public Field(String name, BytesRef bytes, IndexableFieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    if (bytes == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    this.name = name;
    this.fieldsData = bytes;
    this.type = type;
  }

  // TODO: allow direct construction of int, long, float, double value too..?

  /**
   * Create field with String value.
   * @param name field name
   * @param value string value
   * @param type field type
   * @throws IllegalArgumentException if either the name, value or type
   *         is null, or if the field's type is neither indexed() nor stored(), 
   *         or if indexed() is false but storeTermVectors() is true.
   */
  public Field(String name, CharSequence value, IndexableFieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name must not be null");
    }
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    if (!type.stored() && type.indexOptions() == IndexOptions.NONE) {
      throw new IllegalArgumentException("it doesn't make sense to have a field that "
        + "is neither indexed nor stored");
    }
    this.name = name;
    this.fieldsData = value;
    this.type = type;
  }

  /**
   * The value of the field as a String, or null. If null, the Reader value or
   * binary value is used. Exactly one of stringValue(), readerValue(), and
   * binaryValue() must be set.
   */
  @Override
  public String stringValue() {
    if (fieldsData instanceof CharSequence || fieldsData instanceof Number) {
      return fieldsData.toString();
    } else {
      return null;
    }
  }

  @Override
  public CharSequence getCharSequenceValue() {
    return fieldsData instanceof CharSequence ?
        (CharSequence) fieldsData : stringValue();
  }

  /**
   * The value of the field as a Reader, or null. If null, the String value or
   * binary value is used. Exactly one of stringValue(), readerValue(), and
   * binaryValue() must be set.
   */
  @Override
  public Reader readerValue() {
    return fieldsData instanceof Reader ? (Reader) fieldsData : null;
  }
  
  /**
   * The TokenStream for this field to be used when indexing, or null. If null,
   * the Reader value or String value is analyzed to produce the indexed tokens.
   */
  public TokenStream tokenStreamValue() {
    return tokenStream;
  }
  
  /**
   * <p>
   * Expert: change the value of this field. This can be used during indexing to
   * re-use a single Field instance to improve indexing speed by avoiding GC
   * cost of new'ing and reclaiming Field instances. Typically a single
   * {@link Document} instance is re-used as well. This helps most on small
   * documents.
   * </p>
   * 
   * <p>
   * Each Field instance should only be used once within a single
   * {@link Document} instance. See <a
   * href="http://wiki.apache.org/lucene-java/ImproveIndexingSpeed"
   * >ImproveIndexingSpeed</a> for details.
   * </p>
   */
  public void setStringValue(String value) {
    if (!(fieldsData instanceof String)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to String");
    }
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    fieldsData = value;
  }
  
  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setReaderValue(Reader value) {
    if (!(fieldsData instanceof Reader)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Reader");
    }
    fieldsData = value;
  }
  
  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setBytesValue(byte[] value) {
    setBytesValue(new BytesRef(value));
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   *
   * <p>NOTE: the provided BytesRef is not copied so be sure
   * not to change it until you're done with this field.
   */
  public void setBytesValue(BytesRef value) {
    if (!(fieldsData instanceof BytesRef)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to BytesRef");
    }
    if (type.indexOptions() != IndexOptions.NONE) {
      throw new IllegalArgumentException("cannot set a BytesRef value on an indexed field");
    }
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    fieldsData = value;
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setByteValue(byte value) {
    if (!(fieldsData instanceof Byte)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Byte");
    }
    fieldsData = Byte.valueOf(value);
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setShortValue(short value) {
    if (!(fieldsData instanceof Short)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Short");
    }
    fieldsData = Short.valueOf(value);
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setIntValue(int value) {
    if (!(fieldsData instanceof Integer)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Integer");
    }
    fieldsData = Integer.valueOf(value);
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setLongValue(long value) {
    if (!(fieldsData instanceof Long)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Long");
    }
    fieldsData = Long.valueOf(value);
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setFloatValue(float value) {
    if (!(fieldsData instanceof Float)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Float");
    }
    fieldsData = Float.valueOf(value);
  }

  /**
   * Expert: change the value of this field. See 
   * {@link #setStringValue(String)}.
   */
  public void setDoubleValue(double value) {
    if (!(fieldsData instanceof Double)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Double");
    }
    fieldsData = Double.valueOf(value);
  }

  /**
   * Expert: sets the token stream to be used for indexing and causes
   * isIndexed() and isTokenized() to return true. May be combined with stored
   * values from stringValue() or binaryValue()
   */
  public void setTokenStream(TokenStream tokenStream) {
    if (type.indexOptions() == IndexOptions.NONE || !type.tokenized()) {
      throw new IllegalArgumentException("TokenStream fields must be indexed and tokenized");
    }
    this.tokenStream = tokenStream;
  }
  
  @Override
  public String name() {
    return name;
  }

  @Override
  public Number numericValue() {
    if (fieldsData instanceof Number) {
      return (Number) fieldsData;
    } else {
      return null;
    }
  }

  @Override
  public BytesRef binaryValue() {
    if (fieldsData instanceof BytesRef) {
      return (BytesRef) fieldsData;
    } else {
      return null;
    }
  }

  /** Prints a Field for human consumption. */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(type.toString());
    result.append('<');
    result.append(name);
    result.append(':');

    if (fieldsData != null) {
      result.append(fieldsData);
    }

    result.append('>');
    return result.toString();
  }
  
  /** Returns the {@link FieldType} for this field. */
  @Override
  public IndexableFieldType fieldType() {
    return type;
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    if (fieldType().indexOptions() == IndexOptions.NONE) {
      // Not indexed
      return null;
    }

    if (!fieldType().tokenized()) {
      if (stringValue() != null) {
        if (!(reuse instanceof StringTokenStream)) {
          // lazy init the TokenStream as it is heavy to instantiate
          // (attributes,...) if not needed
          reuse = new StringTokenStream();
        }
        ((StringTokenStream) reuse).setValue(stringValue());
        return reuse;
      } else if (binaryValue() != null) {
        if (!(reuse instanceof BinaryTokenStream)) {
          // lazy init the TokenStream as it is heavy to instantiate
          // (attributes,...) if not needed
          reuse = new BinaryTokenStream();
        }
        ((BinaryTokenStream) reuse).setValue(binaryValue());
        return reuse;
      } else {
        throw new IllegalArgumentException("Non-Tokenized Fields must have a String value");
      }
    }

    if (tokenStream != null) {
      return tokenStream;
    } else if (readerValue() != null) {
      return analyzer.tokenStream(name(), readerValue());
    } else if (stringValue() != null) {
      return analyzer.tokenStream(name(), stringValue());
    }

    throw new IllegalArgumentException("Field must have either TokenStream, String, Reader or Number value; got " + this);
  }
  
  private static final class BinaryTokenStream extends TokenStream {
    private final BytesTermAttribute bytesAtt = addAttribute(BytesTermAttribute.class);
    private boolean used = true;
    private BytesRef value;
  
    /** Creates a new TokenStream that returns a BytesRef as single token.
     * <p>Warning: Does not initialize the value, you must call
     * {@link #setValue(BytesRef)} afterwards!
     */
    BinaryTokenStream() {
    }

    public void setValue(BytesRef value) {
      this.value = value;
    }
  
    @Override
    public boolean incrementToken() {
      if (used) {
        return false;
      }
      clearAttributes();
      bytesAtt.setBytesRef(value);
      used = true;
      return true;
    }
  
    @Override
    public void reset() {
      used = false;
    }

    @Override
    public void close() {
      value = null;
    }
  }

  private static final class StringTokenStream extends TokenStream {
    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
    private boolean used = true;
    private String value = null;
    
    /** Creates a new TokenStream that returns a String as single token.
     * <p>Warning: Does not initialize the value, you must call
     * {@link #setValue(String)} afterwards!
     */
    StringTokenStream() {
    }
    
    /** Sets the string value. */
    void setValue(String value) {
      this.value = value;
    }

    @Override
    public boolean incrementToken() {
      if (used) {
        return false;
      }
      clearAttributes();
      termAttribute.append(value);
      offsetAttribute.setOffset(0, value.length());
      used = true;
      return true;
    }

    @Override
    public void end() throws IOException {
      super.end();
      final int finalOffset = value.length();
      offsetAttribute.setOffset(finalOffset, finalOffset);
    }
    
    @Override
    public void reset() {
      used = false;
    }

    @Override
    public void close() {
      value = null;
    }
  }

  /** Specifies whether and how a field should be stored. */
  public static enum Store {

    /** Store the original field value in the index. This is useful for short texts
     * like a document's title which should be displayed with the results. The
     * value is stored in its original form, i.e. no analyzer is used before it is
     * stored.
     */
    YES,

    /** Do not store the field value in the index. */
    NO
  }
}
