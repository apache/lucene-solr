package org.apache.lucene.document;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.IndexWriter; // javadocs
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.Norm; // javadocs
import org.apache.lucene.index.StorableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.FieldInvertState; // javadocs

/**
 * Expert: directly create a field for a document.  Most
 * users should use one of the sugar subclasses: {@link
 * IntField}, {@link LongField}, {@link FloatField}, {@link
 * DoubleField}, {@link ByteDocValuesField}, {@link
 * ShortDocValuesField}, {@link IntDocValuesField}, {@link
 * LongDocValuesField}, {@link PackedLongDocValuesField},
 * {@link FloatDocValuesField}, {@link
 * DoubleDocValuesField}, {@link SortedBytesDocValuesField},
 * {@link DerefBytesDocValuesField}, {@link
 * StraightBytesDocValuesField}, {@link
 * StringField}, {@link TextField}, {@link StoredField}.
 *
 * <p/> A field is a section of a Document. Each field has three
 * parts: name, type and value. Values may be text
 * (String, Reader or pre-analyzed TokenStream), binary
 * (byte[]), or numeric (a Number).  Fields are optionally stored in the
 * index, so that they may be returned with hits on the document.
 *
 * <p/>
 * NOTE: the field type is an {@link IndexableFieldType}.  Making changes
 * to the state of the IndexableFieldType will impact any
 * Field it is used in.  It is strongly recommended that no
 * changes be made after Field instantiation.
 */
public class Field implements IndexableField, StorableField {

  /**
   * Field's type
   */
  protected final FieldType type;

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

  private transient TokenStream internalTokenStream;
  private transient ReusableStringReader internalReader;

  /**
   * Field's boost
   * @see #boost()
   */
  protected float boost = 1.0f;

  /**
   * Expert: creates a field with no initial value.
   * Intended only for custom Field subclasses.
   * @param name field name
   * @param type field type
   * @throws IllegalArgumentException if either the name or type
   *         is null.
   */
  protected Field(String name, FieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    this.name = name;
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
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
  public Field(String name, Reader reader, FieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (reader == null) {
      throw new NullPointerException("reader cannot be null");
    }
    if (type.stored()) {
      throw new IllegalArgumentException("fields with a Reader value cannot be stored");
    }
    if (type.indexed() && !type.tokenized()) {
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
  public Field(String name, TokenStream tokenStream, FieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    if (tokenStream == null) {
      throw new NullPointerException("tokenStream cannot be null");
    }
    if (!type.indexed() || !type.tokenized()) {
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
   * @throws IllegalArgumentException if the field name is null,
   *         or the field's type is indexed()
   * @throws NullPointerException if the type is null
   */
  public Field(String name, byte[] value, FieldType type) {
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
   * @throws IllegalArgumentException if the field name is null,
   *         or the field's type is indexed()
   * @throws NullPointerException if the type is null
   */
  public Field(String name, byte[] value, int offset, int length, FieldType type) {
    this(name, new BytesRef(value, offset, length), type);
  }

  /**
   * Create field with binary value.
   *
   * <p>NOTE: the provided BytesRef is not copied so be sure
   * not to change it until you're done with this field.
   * @param name field name
   * @param bytes BytesRef pointing to binary content (not copied)
   * @param type field type
   * @throws IllegalArgumentException if the field name is null,
   *         or the field's type is indexed()
   * @throws NullPointerException if the type is null
   */
  public Field(String name, BytesRef bytes, FieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    if (type.indexed()) {
      throw new IllegalArgumentException("Fields with BytesRef values cannot be indexed");
    }
    this.fieldsData = bytes;
    this.type = type;
    this.name = name;
  }

  // TODO: allow direct construction of int, long, float, double value too..?

  /**
   * Create field with String value.
   * @param name field name
   * @param value string value
   * @param type field type
   * @throws IllegalArgumentException if either the name or value
   *         is null, or if the field's type is neither indexed() nor stored(), 
   *         or if indexed() is false but storeTermVectors() is true.
   * @throws NullPointerException if the type is null
   */
  public Field(String name, String value, FieldType type) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    if (!type.stored() && !type.indexed()) {
      throw new IllegalArgumentException("it doesn't make sense to have a field that "
        + "is neither indexed nor stored");
    }
    if (!type.indexed() && (type.storeTermVectors())) {
      throw new IllegalArgumentException("cannot store term vector information "
          + "for a field that is not indexed");
    }
    
    this.type = type;
    this.name = name;
    this.fieldsData = value;
  }

  /**
   * The value of the field as a String, or null. If null, the Reader value or
   * binary value is used. Exactly one of stringValue(), readerValue(), and
   * getBinaryValue() must be set.
   */
  public String stringValue() {
    if (fieldsData instanceof String || fieldsData instanceof Number) {
      return fieldsData.toString();
    } else {
      return null;
    }
  }
  
  /**
   * The value of the field as a Reader, or null. If null, the String value or
   * binary value is used. Exactly one of stringValue(), readerValue(), and
   * getBinaryValue() must be set.
   */
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
    if (type.indexed()) {
      throw new IllegalArgumentException("cannot set a Reader value on an indexed field");
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
   * values from stringValue() or getBinaryValue()
   */
  public void setTokenStream(TokenStream tokenStream) {
    if (!type.indexed() || !type.tokenized()) {
      throw new IllegalArgumentException("TokenStream fields must be indexed and tokenized");
    }
    if (type.numericType() != null) {
      throw new IllegalArgumentException("cannot set private TokenStream on numeric fields");
    }
    this.tokenStream = tokenStream;
  }
  
  public String name() {
    return name;
  }
  
  /** 
   * {@inheritDoc}
   * <p>
   * The default value is <code>1.0f</code> (no boost).
   * @see #setBoost(float)
   */
  public float boost() {
    return boost;
  }

  /** 
   * Sets the boost factor on this field.
   * @throws IllegalArgumentException if this field is not indexed, 
   *         or if it omits norms. 
   * @see #boost()
   */
  public void setBoost(float boost) {
    if (boost != 1.0f) {
      if (type.indexed() == false || type.omitNorms()) {
        throw new IllegalArgumentException("You cannot set an index-time boost on an unindexed field, or one that omits norms");
      }
    }
    this.boost = boost;
  }

  public Number numericValue() {
    if (fieldsData instanceof Number) {
      return (Number) fieldsData;
    } else {
      return null;
    }
  }

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
  public FieldType fieldType() {
    return type;
  }

  public TokenStream tokenStream(Analyzer analyzer) throws IOException {
    if (!fieldType().indexed()) {
      return null;
    }

    final NumericType numericType = fieldType().numericType();
    if (numericType != null) {
      if (!(internalTokenStream instanceof NumericTokenStream)) {
        // lazy init the TokenStream as it is heavy to instantiate
        // (attributes,...) if not needed (stored field loading)
        internalTokenStream = new NumericTokenStream(type.numericPrecisionStep());
      }
      final NumericTokenStream nts = (NumericTokenStream) internalTokenStream;
      // initialize value in TokenStream
      final Number val = (Number) fieldsData;
      switch (numericType) {
      case INT:
        nts.setIntValue(val.intValue());
        break;
      case LONG:
        nts.setLongValue(val.longValue());
        break;
      case FLOAT:
        nts.setFloatValue(val.floatValue());
        break;
      case DOUBLE:
        nts.setDoubleValue(val.doubleValue());
        break;
      default:
        throw new AssertionError("Should never get here");
      }
      return internalTokenStream;
    }

    if (!fieldType().tokenized()) {
      if (stringValue() == null) {
        throw new IllegalArgumentException("Non-Tokenized Fields must have a String value");
      }
      if (!(internalTokenStream instanceof StringTokenStream)) {
        // lazy init the TokenStream as it is heavy to instantiate
        // (attributes,...) if not needed (stored field loading)
        internalTokenStream = new StringTokenStream();
      }
      ((StringTokenStream) internalTokenStream).setValue(stringValue());
      return internalTokenStream;
    }

    if (tokenStream != null) {
      return tokenStream;
    } else if (readerValue() != null) {
      return analyzer.tokenStream(name(), readerValue());
    } else if (stringValue() != null) {
      if (internalReader == null) {
        internalReader = new ReusableStringReader();
      }
      internalReader.setValue(stringValue());
      return analyzer.tokenStream(name(), internalReader);
    }

    throw new IllegalArgumentException("Field must have either TokenStream, String, Reader or Number value");
  }
  
  static final class ReusableStringReader extends Reader {
    private int pos = 0, size = 0;
    private String s = null;
    
    void setValue(String s) {
      this.s = s;
      this.size = s.length();
      this.pos = 0;
    }
    
    @Override
    public int read() {
      if (pos < size) {
        return s.charAt(pos++);
      } else {
        s = null;
        return -1;
      }
    }
    
    @Override
    public int read(char[] c, int off, int len) {
      if (pos < size) {
        len = Math.min(len, size-pos);
        s.getChars(pos, pos+len, c, off);
        pos += len;
        return len;
      } else {
        s = null;
        return -1;
      }
    }
    
    @Override
    public void close() {
      pos = size; // this prevents NPE when reading after close!
      s = null;
    }
  }
  
  static final class StringTokenStream extends TokenStream {
    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
    private boolean used = false;
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
    public void end() {
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
