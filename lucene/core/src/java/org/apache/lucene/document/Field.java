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
import org.apache.lucene.index.Norm;
import org.apache.lucene.util.BytesRef;

/**
 * Expert: directly creata a field for a document.  Most
 * users should use one of the sugar subclasses: {@link
 * IntField}, {@link LongField}, {@link FloatField}, {@link
 * DoubleField}, {@link DocValuesField}, {@link
 * StringField}, {@link TextField}, {@link StoredField}.
 *
 * <p/> A field is a section of a Document. Each field has three
 * parts: name, type andvalue. Values may be text
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
   * Expert: change the value of this field. See <a
   * href="#setValue(java.lang.String)">setValue(String)</a>.
   */
  public void setReaderValue(Reader value) {
    if (!(fieldsData instanceof Reader)) {
      throw new IllegalArgumentException("cannot change value type from " + fieldsData.getClass().getSimpleName() + " to Reader");
    }
    fieldsData = value;
  }
  
  /**
   * Expert: change the value of this field. See <a
   * href="#setValue(java.lang.String)">setValue(String)</a>.
   */
  public void setBytesValue(byte[] value) {
    setBytesValue(new BytesRef(value));
  }

  /**
   * Expert: change the value of this field. See <a
   * href="#setValue(java.lang.String)">setValue(String)</a>.
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

  
  //
  // Deprecated transition API below:
  //

  /** Specifies whether and how a field should be stored.
   *
   *  @deprecated This is here only to ease transition from
   *  the pre-4.0 APIs. */
  @Deprecated
  public static enum Store {

    /** Store the original field value in the index. This is useful for short texts
     * like a document's title which should be displayed with the results. The
     * value is stored in its original form, i.e. no analyzer is used before it is
     * stored.
     */
    YES {
      @Override
      public boolean isStored() { return true; }
    },

    /** Do not store the field value in the index. */
    NO {
      @Override
      public boolean isStored() { return false; }
    };

    public abstract boolean isStored();
  }

  /** Specifies whether and how a field should be indexed.
   *
   *  @deprecated This is here only to ease transition from
   *  the pre-4.0 APIs. */
  @Deprecated
  public static enum Index {

    /** Do not index the field value. This field can thus not be searched,
     * but one can still access its contents provided it is
     * {@link Field.Store stored}. */
    NO {
      @Override
      public boolean isIndexed()  { return false; }
      @Override
      public boolean isAnalyzed() { return false; }
      @Override
      public boolean omitNorms()  { return true;  }   
    },

    /** Index the tokens produced by running the field's
     * value through an Analyzer.  This is useful for
     * common text. */
    ANALYZED {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return true;  }
      @Override
      public boolean omitNorms()  { return false; }   	
    },

    /** Index the field's value without using an Analyzer, so it can be searched.
     * As no analyzer is used the value will be stored as a single term. This is
     * useful for unique Ids like product numbers.
     */
    NOT_ANALYZED {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return false; }
      @Override
      public boolean omitNorms()  { return false; }   	
    },

    /** Expert: Index the field's value without an Analyzer,
     * and also disable the indexing of norms.  Note that you
     * can also separately enable/disable norms by calling
     * {@link FieldType#setOmitNorms}.  No norms means that
     * index-time field and document boosting and field
     * length normalization are disabled.  The benefit is
     * less memory usage as norms take up one byte of RAM
     * per indexed field for every document in the index,
     * during searching.  Note that once you index a given
     * field <i>with</i> norms enabled, disabling norms will
     * have no effect.  In other words, for this to have the
     * above described effect on a field, all instances of
     * that field must be indexed with NOT_ANALYZED_NO_NORMS
     * from the beginning. */
    NOT_ANALYZED_NO_NORMS {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return false; }
      @Override
      public boolean omitNorms()  { return true;  }   	
    },

    /** Expert: Index the tokens produced by running the
     *  field's value through an Analyzer, and also
     *  separately disable the storing of norms.  See
     *  {@link #NOT_ANALYZED_NO_NORMS} for what norms are
     *  and why you may want to disable them. */
    ANALYZED_NO_NORMS {
      @Override
      public boolean isIndexed()  { return true;  }
      @Override
      public boolean isAnalyzed() { return true;  }
      @Override
      public boolean omitNorms()  { return true;  }   	
    };

    /** Get the best representation of the index given the flags. */
    public static Index toIndex(boolean indexed, boolean analyzed) {
      return toIndex(indexed, analyzed, false);
    }

    /** Expert: Get the best representation of the index given the flags. */
    public static Index toIndex(boolean indexed, boolean analyzed, boolean omitNorms) {

      // If it is not indexed nothing else matters
      if (!indexed) {
        return Index.NO;
      }

      // typical, non-expert
      if (!omitNorms) {
        if (analyzed) {
          return Index.ANALYZED;
        }
        return Index.NOT_ANALYZED;
      }

      // Expert: Norms omitted
      if (analyzed) {
        return Index.ANALYZED_NO_NORMS;
      }
      return Index.NOT_ANALYZED_NO_NORMS;
    }

    public abstract boolean isIndexed();
    public abstract boolean isAnalyzed();
    public abstract boolean omitNorms();  	
  }

  /** Specifies whether and how a field should have term vectors.
   *
   *  @deprecated This is here only to ease transition from
   *  the pre-4.0 APIs. */
  @Deprecated
  public static enum TermVector {
    
    /** Do not store term vectors. 
     */
    NO {
      @Override
      public boolean isStored()      { return false; }
      @Override
      public boolean withPositions() { return false; }
      @Override
      public boolean withOffsets()   { return false; }
    },
    
    /** Store the term vectors of each document. A term vector is a list
     * of the document's terms and their number of occurrences in that document. */
    YES {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return false; }
      @Override
      public boolean withOffsets()   { return false; }
    },
    
    /**
     * Store the term vector + token position information
     * 
     * @see #YES
     */ 
    WITH_POSITIONS {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return true;  }
      @Override
      public boolean withOffsets()   { return false; }
    },
    
    /**
     * Store the term vector + Token offset information
     * 
     * @see #YES
     */ 
    WITH_OFFSETS {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return false; }
      @Override
      public boolean withOffsets()   { return true;  }
    },
    
    /**
     * Store the term vector + Token position and offset information
     * 
     * @see #YES
     * @see #WITH_POSITIONS
     * @see #WITH_OFFSETS
     */ 
    WITH_POSITIONS_OFFSETS {
      @Override
      public boolean isStored()      { return true;  }
      @Override
      public boolean withPositions() { return true;  }
      @Override
      public boolean withOffsets()   { return true;  }
    };

    /** Get the best representation of a TermVector given the flags. */
    public static TermVector toTermVector(boolean stored, boolean withOffsets, boolean withPositions) {

      // If it is not stored, nothing else matters.
      if (!stored) {
        return TermVector.NO;
      }

      if (withOffsets) {
        if (withPositions) {
          return Field.TermVector.WITH_POSITIONS_OFFSETS;
        }
        return Field.TermVector.WITH_OFFSETS;
      }

      if (withPositions) {
        return Field.TermVector.WITH_POSITIONS;
      }
      return Field.TermVector.YES;
    }

    public abstract boolean isStored();
    public abstract boolean withPositions();
    public abstract boolean withOffsets();
  }

  /** Translates the pre-4.0 enums for specifying how a
   *  field should be indexed into the 4.0 {@link FieldType}
   *  approach.
   *
   * @deprecated This is here only to ease transition from
   * the pre-4.0 APIs.
   */
  @Deprecated
  public static final FieldType translateFieldType(Store store, Index index, TermVector termVector) {
    final FieldType ft = new FieldType();

    ft.setStored(store == Store.YES);

    switch(index) {
    case ANALYZED:
      ft.setIndexed(true);
      ft.setTokenized(true);
      break;
    case ANALYZED_NO_NORMS:
      ft.setIndexed(true);
      ft.setTokenized(true);
      ft.setOmitNorms(true);
      break;
    case NOT_ANALYZED:
      ft.setIndexed(true);
      break;
    case NOT_ANALYZED_NO_NORMS:
      ft.setIndexed(true);
      ft.setOmitNorms(true);
      break;
    case NO:
      break;
    }

    switch(termVector) {
    case NO:
      break;
    case YES:
      ft.setStoreTermVectors(true);
      break;
    case WITH_POSITIONS:
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorPositions(true);
      break;
    case WITH_OFFSETS:
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(true);
      break;
    case WITH_POSITIONS_OFFSETS:
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorPositions(true);
      ft.setStoreTermVectorOffsets(true);
      break;
    }
    ft.freeze();
    return ft;
  }

  /**
   * Create a field by specifying its name, value and how it will
   * be saved in the index. Term vectors will not be stored in the index.
   * 
   * @param name The name of the field
   * @param value The string to process
   * @param store Whether <code>value</code> should be stored in the index
   * @param index Whether the field should be indexed, and if so, if it should
   *  be tokenized before indexing 
   * @throws NullPointerException if name or value is <code>null</code>
   * @throws IllegalArgumentException if the field is neither stored nor indexed 
   *
   * @deprecated Use {@link StringField}, {@link TextField} instead. */
  @Deprecated
  public Field(String name, String value, Store store, Index index) {
    this(name, value, translateFieldType(store, index, TermVector.NO));
  }

  /**
   * Create a field by specifying its name, value and how it will
   * be saved in the index.
   * 
   * @param name The name of the field
   * @param value The string to process
   * @param store Whether <code>value</code> should be stored in the index
   * @param index Whether the field should be indexed, and if so, if it should
   *  be tokenized before indexing 
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or value is <code>null</code>
   * @throws IllegalArgumentException in any of the following situations:
   * <ul> 
   *  <li>the field is neither stored nor indexed</li> 
   *  <li>the field is not indexed but termVector is <code>TermVector.YES</code></li>
   * </ul> 
   *
   * @deprecated Use {@link StringField}, {@link TextField} instead. */
  @Deprecated
  public Field(String name, String value, Store store, Index index, TermVector termVector) {  
    this(name, value, translateFieldType(store, index, termVector));
  }

  /**
   * Create a tokenized and indexed field that is not stored. Term vectors will
   * not be stored.  The Reader is read only when the Document is added to the index,
   * i.e. you may not close the Reader until {@link IndexWriter#addDocument}
   * has been called.
   * 
   * @param name The name of the field
   * @param reader The reader with the content
   * @throws NullPointerException if name or reader is <code>null</code>
   *
   * @deprecated Use {@link TextField} instead.
   */
  @Deprecated
  public Field(String name, Reader reader) {
    this(name, reader, TermVector.NO);
  }

  /**
   * Create a tokenized and indexed field that is not stored, optionally with 
   * storing term vectors.  The Reader is read only when the Document is added to the index,
   * i.e. you may not close the Reader until {@link IndexWriter#addDocument}
   * has been called.
   * 
   * @param name The name of the field
   * @param reader The reader with the content
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or reader is <code>null</code>
   *
   * @deprecated Use {@link TextField} instead.
   */ 
  @Deprecated
  public Field(String name, Reader reader, TermVector termVector) {
    this(name, reader, translateFieldType(Store.NO, Index.ANALYZED, termVector));
  }

  /**
   * Create a tokenized and indexed field that is not stored. Term vectors will
   * not be stored. This is useful for pre-analyzed fields.
   * The TokenStream is read only when the Document is added to the index,
   * i.e. you may not close the TokenStream until {@link IndexWriter#addDocument}
   * has been called.
   * 
   * @param name The name of the field
   * @param tokenStream The TokenStream with the content
   * @throws NullPointerException if name or tokenStream is <code>null</code>
   *
   * @deprecated Use {@link TextField} instead
   */ 
  @Deprecated
  public Field(String name, TokenStream tokenStream) {
    this(name, tokenStream, TermVector.NO);
  }

  /**
   * Create a tokenized and indexed field that is not stored, optionally with 
   * storing term vectors.  This is useful for pre-analyzed fields.
   * The TokenStream is read only when the Document is added to the index,
   * i.e. you may not close the TokenStream until {@link IndexWriter#addDocument}
   * has been called.
   * 
   * @param name The name of the field
   * @param tokenStream The TokenStream with the content
   * @param termVector Whether term vector should be stored
   * @throws NullPointerException if name or tokenStream is <code>null</code>
   *
   * @deprecated Use {@link TextField} instead
   */ 
  @Deprecated
  public Field(String name, TokenStream tokenStream, TermVector termVector) {
    this(name, tokenStream, translateFieldType(Store.NO, Index.ANALYZED, termVector));
  }

  /**
   * Create a stored field with binary value. Optionally the value may be compressed.
   * 
   * @param name The name of the field
   * @param value The binary value
   *
   * @deprecated Use {@link StoredField} instead.
   */
  @Deprecated
  public Field(String name, byte[] value) {
    this(name, value, translateFieldType(Store.YES, Index.NO, TermVector.NO));
  }

  /**
   * Create a stored field with binary value. Optionally the value may be compressed.
   * 
   * @param name The name of the field
   * @param value The binary value
   * @param offset Starting offset in value where this Field's bytes are
   * @param length Number of bytes to use for this Field, starting at offset
   *
   * @deprecated Use {@link StoredField} instead.
   */
  @Deprecated
  public Field(String name, byte[] value, int offset, int length) {
    this(name, value, offset, length, translateFieldType(Store.YES, Index.NO, TermVector.NO));
  }
}
