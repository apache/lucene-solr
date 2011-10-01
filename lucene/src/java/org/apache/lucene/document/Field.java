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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.util.BytesRef;

/**
 * A field is a section of a Document. Each field has two parts, a name and a
 * value. Values may be free text, provided as a String or as a Reader, or they
 * may be atomic keywords, which are not further processed. Such keywords may be
 * used to represent dates, urls, etc. Fields are optionally stored in the
 * index, so that they may be returned with hits on the document.
 * <p/>
 * Note, Field instances are instantiated with a {@link IndexableFieldType}.  Making changes
 * to the state of the FieldType will impact any Field it is used in, therefore
 * it is strongly recommended that no changes are made after Field instantiation.
 */
public class Field implements IndexableField {
  
  protected IndexableFieldType type;
  protected String name = "body";
  // the data object for all different kind of field values
  protected Object fieldsData;
  // pre-analyzed tokenStream for indexed fields
  protected TokenStream tokenStream;
  // length/offset for all primitive types
  protected PerDocFieldValues docValues;
  
  protected float boost = 1.0f;

  public Field(String name, IndexableFieldType type) {
    this.name = name;
    this.type = type;
  }
  
  public Field(String name, Reader reader, IndexableFieldType type) {
    if (name == null) {
      throw new NullPointerException("name cannot be null");
    }
    if (reader == null) {
      throw new NullPointerException("reader cannot be null");
    }
    if (type.indexed() && !type.tokenized()) {
      throw new IllegalArgumentException("Non-tokenized fields must use String values");
    }
    
    this.name = name;
    this.fieldsData = reader;
    this.type = type;
  }
  
  public Field(String name, TokenStream tokenStream, IndexableFieldType type) {
    if (name == null) {
      throw new NullPointerException("name cannot be null");
    }
    if (tokenStream == null) {
      throw new NullPointerException("tokenStream cannot be null");
    }
    if (type.indexed() && !type.tokenized()) {
      throw new IllegalArgumentException("Non-tokenized fields must use String values");
    }
    
    this.name = name;
    this.fieldsData = null;
    this.tokenStream = tokenStream;
    this.type = type;
  }
  
  public Field(String name, byte[] value, IndexableFieldType type) {
    this(name, value, 0, value.length, type);
  }

  public Field(String name, byte[] value, int offset, int length, IndexableFieldType type) {
    this(name, new BytesRef(value, offset, length), type);
  }

  public Field(String name, BytesRef bytes, IndexableFieldType type) {
    if (type.indexed() && !type.tokenized()) {
      throw new IllegalArgumentException("Non-tokenized fields must use String values");
    }

    this.fieldsData = bytes;
    this.type = type;
    this.name = name;
  }
  
  public Field(String name, String value, IndexableFieldType type) {
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
    if (!type.indexed() && !type.tokenized() && (type.storeTermVectors())) {
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
  public void setValue(String value) {
    if (isBinary()) {
      throw new IllegalArgumentException(
          "cannot set a String value on a binary field");
    }
    fieldsData = value;
  }
  
  /**
   * Expert: change the value of this field. See <a
   * href="#setValue(java.lang.String)">setValue(String)</a>.
   */
  public void setValue(Reader value) {
    if (isBinary()) {
      throw new IllegalArgumentException(
          "cannot set a Reader value on a binary field");
    }
    if (type.stored()) {
      throw new IllegalArgumentException(
          "cannot set a Reader value on a stored field");
    }
    fieldsData = value;
  }
  
  /**
   * Expert: change the value of this field. See <a
   * href="#setValue(java.lang.String)">setValue(String)</a>.
   */
  public void setValue(byte[] value) {
    if (!isBinary()) {
      throw new IllegalArgumentException(
          "cannot set a byte[] value on a non-binary field");
    }
    fieldsData = new BytesRef(value);
  }
  
  /**
   * Expert: sets the token stream to be used for indexing and causes
   * isIndexed() and isTokenized() to return true. May be combined with stored
   * values from stringValue() or getBinaryValue()
   */
  public void setTokenStream(TokenStream tokenStream) {
    if (!type.indexed() || !type.tokenized()) {
      throw new IllegalArgumentException(
          "cannot set token stream on non indexed and tokenized field");
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
   * default, in the {@link org.apache.lucene.search.similarities.Similarity#computeNorm(FieldInvertState)} method, 
   * the boost value is multiplied by the length normalization factor and then
   * rounded by {@link org.apache.lucene.search.similarities.DefaultSimilarity#encodeNormValue(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   *
   * @see org.apache.lucene.search.similarities.Similarity#computeNorm(FieldInvertState)
   * @see org.apache.lucene.search.similarities.DefaultSimilarity#encodeNormValue(float)
   */
  public void setBoost(float boost) {
    this.boost = boost;
  }
  
  public boolean numeric() {
    return false;
  }

  public Number numericValue() {
    return null;
  }

  public NumericField.DataType numericDataType() {
    return null;
  }
  
  public BytesRef binaryValue() {
    if (!isBinary()) {
      return null;
    } else {
      return (BytesRef) fieldsData;
    }
  }
  
  /** methods from inner IndexableFieldType */
  
  public boolean isBinary() {
    return fieldsData instanceof BytesRef;
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
  
  public void setDocValues(PerDocFieldValues docValues) {
    this.docValues = docValues;
  }

  @Override
  public PerDocFieldValues docValues() {
    return null;
  }
  
  @Override
  public ValueType docValuesType() {
    return null;
  }

  /** Returns FieldType for this field. */
  public IndexableFieldType fieldType() {
    return type;
  }

  /**
   * {@inheritDoc}
   */
  public TokenStream tokenStream(Analyzer analyzer) throws IOException {
    if (!fieldType().indexed()) {
      return null;
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

    throw new IllegalArgumentException("Field must have either TokenStream, String or Reader value");
  }
}
