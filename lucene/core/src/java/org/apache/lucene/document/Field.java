package org.apache.lucene.document;

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
import java.io.Reader;
import java.io.StringReader;

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
public class Field implements IndexableField {

  protected final FieldType type;
  protected final String name;

  // Field's value:
  protected Object fieldsData;

  // Pre-analyzed tokenStream for indexed fields; this is
  // separate from fieldsData because you are allowed to
  // have both; eg maybe field has a String value but you
  // customize how it's tokenized:
  protected TokenStream tokenStream;

  protected transient NumericTokenStream numericTokenStream;

  protected float boost = 1.0f;

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
   */
  public Field(String name, byte[] value, FieldType type) {
    this(name, value, 0, value.length, type);
  }

  /**
   * Create field with binary value.
   */
  public Field(String name, byte[] value, int offset, int length, FieldType type) {
    this(name, new BytesRef(value, offset, length), type);
  }

  /**
   * Create field with binary value.
   *
   * <p>NOTE: the provided BytesRef is not copied so be sure
   * not to change it until you're done with this field.
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
    return fieldsData instanceof String ? (String) fieldsData : null;
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
   * The TokesStream for this field to be used when indexing, or null. If null,
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

  public void setByteValue(byte value) {
    if (!(fieldsData instanceof Byte)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Byte");
    }
    if (numericTokenStream != null) {
      numericTokenStream.setIntValue(value);
    }
    fieldsData = Byte.valueOf(value);
  }

  public void setShortValue(short value) {
    if (!(fieldsData instanceof Short)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Short");
    }
    if (numericTokenStream != null) {
      numericTokenStream.setIntValue(value);
    }
    fieldsData = Short.valueOf(value);
  }

  public void setIntValue(int value) {
    if (!(fieldsData instanceof Integer)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Integer");
    }
    if (numericTokenStream != null) {
      numericTokenStream.setIntValue(value);
    }
    fieldsData = Integer.valueOf(value);
  }

  public void setLongValue(long value) {
    if (!(fieldsData instanceof Long)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Long");
    }
    if (numericTokenStream != null) {
      numericTokenStream.setLongValue(value);
    }
    fieldsData = Long.valueOf(value);
  }

  public void setFloatValue(float value) {
    if (!(fieldsData instanceof Float)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Float");
    }
    if (numericTokenStream != null) {
      numericTokenStream.setFloatValue(value);
    }
    fieldsData = Float.valueOf(value);
  }

  public void setDoubleValue(double value) {
    if (!(fieldsData instanceof Double)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Double");
    }
    if (numericTokenStream != null) {
      numericTokenStream.setDoubleValue(value);
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
  
  public float boost() {
    return boost;
  }

  /** Sets the boost factor hits on this field.  This value will be
   * multiplied into the score of all hits on this this field of this
   * document.
   *
   * <p>The boost is used to compute the norm factor for the field.  By
   * default, in the {@link org.apache.lucene.search.similarities.Similarity#computeNorm(FieldInvertState, Norm)} method, 
   * the boost value is multiplied by the length normalization factor and then
   * rounded by {@link org.apache.lucene.search.similarities.DefaultSimilarity#encodeNormValue(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   *
   * @see org.apache.lucene.search.similarities.Similarity#computeNorm(FieldInvertState, Norm)
   * @see org.apache.lucene.search.similarities.DefaultSimilarity#encodeNormValue(float)
   */
  public void setBoost(float boost) {
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

  /**
   * {@inheritDoc}
   */
  public TokenStream tokenStream(Analyzer analyzer) throws IOException {
    if (!fieldType().indexed()) {
      return null;
    }

    final NumericType numericType = fieldType().numericType();
    if (numericType != null) {
      if (numericTokenStream == null) {
        // lazy init the TokenStream as it is heavy to instantiate
        // (attributes,...) if not needed (stored field loading)
        numericTokenStream = new NumericTokenStream(type.numericPrecisionStep());
        // initialize value in TokenStream
        final Number val = (Number) fieldsData;
        switch (numericType) {
        case INT:
          numericTokenStream.setIntValue(val.intValue());
          break;
        case LONG:
          numericTokenStream.setLongValue(val.longValue());
          break;
        case FLOAT:
          numericTokenStream.setFloatValue(val.floatValue());
          break;
        case DOUBLE:
          numericTokenStream.setDoubleValue(val.doubleValue());
          break;
        default:
          assert false : "Should never get here";
        }
      } else {
        // OK -- previously cached and we already updated if
        // setters were called.
      }

      return numericTokenStream;
    }

    if (!fieldType().tokenized()) {
      if (stringValue() == null) {
        throw new IllegalArgumentException("Non-Tokenized Fields must have a String value");
      }

      return new TokenStream() {
        CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
        boolean used;

        @Override
        public boolean incrementToken() throws IOException {
          if (used) {
            return false;
          }
          termAttribute.setEmpty().append(stringValue());
          offsetAttribute.setOffset(0, stringValue().length());
          used = true;
          return true;
        }

        @Override
        public void reset() throws IOException {
          used = false;
        }
      };
    }

    if (tokenStream != null) {
      return tokenStream;
    } else if (readerValue() != null) {
      return analyzer.tokenStream(name(), readerValue());
    } else if (stringValue() != null) {
      return analyzer.tokenStream(name(), new StringReader(stringValue()));
    }

    throw new IllegalArgumentException("Field must have either TokenStream, String, Reader or Number value");
  }
}
