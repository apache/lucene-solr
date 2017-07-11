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
package org.apache.solr.legacy;


import java.util.Objects;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

/**
 * <b>Expert:</b> This class provides a {@link TokenStream}
 * for indexing numeric values that can be used by {@link
 * org.apache.solr.legacy.LegacyNumericRangeQuery}.
 *
 * <p>Note that for simple usage, {@link org.apache.solr.legacy.LegacyIntField}, {@link
 * org.apache.solr.legacy.LegacyLongField}, {@link org.apache.solr.legacy.LegacyFloatField} or {@link org.apache.solr.legacy.LegacyDoubleField} is
 * recommended.  These fields disable norms and
 * term freqs, as they are not usually needed during
 * searching.  If you need to change these settings, you
 * should use this class.
 *
 * <p>Here's an example usage, for an <code>int</code> field:
 *
 * <pre class="prettyprint">
 *  FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
 *  fieldType.setOmitNorms(true);
 *  fieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
 *  Field field = new Field(name, new LegacyNumericTokenStream(precisionStep).setIntValue(value), fieldType);
 *  document.add(field);
 * </pre>
 *
 * <p>For optimal performance, re-use the TokenStream and Field instance
 * for more than one document:
 *
 * <pre class="prettyprint">
 *  LegacyNumericTokenStream stream = new LegacyNumericTokenStream(precisionStep);
 *  FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
 *  fieldType.setOmitNorms(true);
 *  fieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
 *  Field field = new Field(name, stream, fieldType);
 *  Document document = new Document();
 *  document.add(field);
 *
 *  for(all documents) {
 *    stream.setIntValue(value)
 *    writer.addDocument(document);
 *  }
 * </pre>
 *
 * <p>This stream is not intended to be used in analyzers;
 * it's more for iterating the different precisions during
 * indexing a specific numeric value.</p>

 * <p><b>NOTE</b>: as token streams are only consumed once
 * the document is added to the index, if you index more
 * than one numeric field, use a separate <code>LegacyNumericTokenStream</code>
 * instance for each.</p>
 *
 * <p>See {@link org.apache.solr.legacy.LegacyNumericRangeQuery} for more details on the
 * <a
 * href="LegacyNumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>
 * parameter as well as how numeric fields work under the hood.</p>
 *
 * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
 *
 * @since 2.9
 */
@Deprecated
public final class LegacyNumericTokenStream extends TokenStream {

  /** The full precision token gets this token type assigned. */
  public static final String TOKEN_TYPE_FULL_PREC  = "fullPrecNumeric";

  /** The lower precision tokens gets this token type assigned. */
  public static final String TOKEN_TYPE_LOWER_PREC = "lowerPrecNumeric";
  
  /** <b>Expert:</b> Use this attribute to get the details of the currently generated token.
   * @lucene.experimental
   * @since 4.0
   */
  public interface LegacyNumericTermAttribute extends Attribute {
    /** Returns current shift value, undefined before first token */
    int getShift();
    /** Returns current token's raw value as {@code long} with all {@link #getShift} applied, undefined before first token */
    long getRawValue();
    /** Returns value size in bits (32 for {@code float}, {@code int}; 64 for {@code double}, {@code long}) */
    int getValueSize();
    
    /** <em>Don't call this method!</em>
      * @lucene.internal */
    void init(long value, int valSize, int precisionStep, int shift);

    /** <em>Don't call this method!</em>
      * @lucene.internal */
    void setShift(int shift);

    /** <em>Don't call this method!</em>
      * @lucene.internal */
    int incShift();
  }
  
  // just a wrapper to prevent adding CTA
  private static final class NumericAttributeFactory extends AttributeFactory {
    private final AttributeFactory delegate;

    NumericAttributeFactory(AttributeFactory delegate) {
      this.delegate = delegate;
    }
  
    @Override
    public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
      if (CharTermAttribute.class.isAssignableFrom(attClass))
        throw new IllegalArgumentException("LegacyNumericTokenStream does not support CharTermAttribute.");
      return delegate.createAttributeInstance(attClass);
    }
  }

  /** Implementation of {@link org.apache.solr.legacy.LegacyNumericTokenStream.LegacyNumericTermAttribute}.
   * @lucene.internal
   * @since 4.0
   */
  public static final class LegacyNumericTermAttributeImpl extends AttributeImpl implements LegacyNumericTermAttribute,TermToBytesRefAttribute {
    private long value = 0L;
    private int valueSize = 0, shift = 0, precisionStep = 0;
    private BytesRefBuilder bytes = new BytesRefBuilder();
    
    /** 
     * Creates, but does not yet initialize this attribute instance
     * @see #init(long, int, int, int)
     */
    public LegacyNumericTermAttributeImpl() {}

    @Override
    public BytesRef getBytesRef() {
      assert valueSize == 64 || valueSize == 32;
      if (shift >= valueSize) {
        bytes.clear();
      } else if (valueSize == 64) {
        LegacyNumericUtils.longToPrefixCoded(value, shift, bytes);
      } else {
        LegacyNumericUtils.intToPrefixCoded((int) value, shift, bytes);
      }
      return bytes.get();
    }

    @Override
    public int getShift() { return shift; }
    @Override
    public void setShift(int shift) { this.shift = shift; }
    @Override
    public int incShift() {
      return (shift += precisionStep);
    }

    @Override
    public long getRawValue() { return value  & ~((1L << shift) - 1L); }
    @Override
    public int getValueSize() { return valueSize; }

    @Override
    public void init(long value, int valueSize, int precisionStep, int shift) {
      this.value = value;
      this.valueSize = valueSize;
      this.precisionStep = precisionStep;
      this.shift = shift;
    }

    @Override
    public void clear() {
      // this attribute has no contents to clear!
      // we keep it untouched as it's fully controlled by outer class.
    }
    
    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(TermToBytesRefAttribute.class, "bytes", getBytesRef());
      reflector.reflect(LegacyNumericTermAttribute.class, "shift", shift);
      reflector.reflect(LegacyNumericTermAttribute.class, "rawValue", getRawValue());
      reflector.reflect(LegacyNumericTermAttribute.class, "valueSize", valueSize);
    }
  
    @Override
    public void copyTo(AttributeImpl target) {
      final LegacyNumericTermAttribute a = (LegacyNumericTermAttribute) target;
      a.init(value, valueSize, precisionStep, shift);
    }
    
    @Override
    public LegacyNumericTermAttributeImpl clone() {
      LegacyNumericTermAttributeImpl t = (LegacyNumericTermAttributeImpl)super.clone();
      // Do a deep clone
      t.bytes = new BytesRefBuilder();
      t.bytes.copyBytes(getBytesRef());
      return t;
    }

    @Override
    public int hashCode() {
      return Objects.hash(precisionStep, shift, value, valueSize);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      LegacyNumericTermAttributeImpl other = (LegacyNumericTermAttributeImpl) obj;
      if (precisionStep != other.precisionStep) return false;
      if (shift != other.shift) return false;
      if (value != other.value) return false;
      if (valueSize != other.valueSize) return false;
      return true;
    }
  }
  
  /**
   * Creates a token stream for numeric values using the default <code>precisionStep</code>
   * {@link org.apache.solr.legacy.LegacyNumericUtils#PRECISION_STEP_DEFAULT} (16). The stream is not yet initialized,
   * before using set a value using the various set<em>???</em>Value() methods.
   */
  public LegacyNumericTokenStream() {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, LegacyNumericUtils.PRECISION_STEP_DEFAULT);
  }
  
  /**
   * Creates a token stream for numeric values with the specified
   * <code>precisionStep</code>. The stream is not yet initialized,
   * before using set a value using the various set<em>???</em>Value() methods.
   */
  public LegacyNumericTokenStream(final int precisionStep) {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, precisionStep);
  }

  /**
   * Expert: Creates a token stream for numeric values with the specified
   * <code>precisionStep</code> using the given
   * {@link org.apache.lucene.util.AttributeFactory}.
   * The stream is not yet initialized,
   * before using set a value using the various set<em>???</em>Value() methods.
   */
  public LegacyNumericTokenStream(AttributeFactory factory, final int precisionStep) {
    super(new NumericAttributeFactory(factory));
    if (precisionStep < 1)
      throw new IllegalArgumentException("precisionStep must be >=1");
    this.precisionStep = precisionStep;
    numericAtt.setShift(-precisionStep);
  }

  /**
   * Initializes the token stream with the supplied <code>long</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new LegacyNumericTokenStream(precisionStep).setLongValue(value))</code>
   */
  public LegacyNumericTokenStream setLongValue(final long value) {
    numericAtt.init(value, valSize = 64, precisionStep, -precisionStep);
    return this;
  }
  
  /**
   * Initializes the token stream with the supplied <code>int</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new LegacyNumericTokenStream(precisionStep).setIntValue(value))</code>
   */
  public LegacyNumericTokenStream setIntValue(final int value) {
    numericAtt.init(value, valSize = 32, precisionStep, -precisionStep);
    return this;
  }
  
  /**
   * Initializes the token stream with the supplied <code>double</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new LegacyNumericTokenStream(precisionStep).setDoubleValue(value))</code>
   */
  public LegacyNumericTokenStream setDoubleValue(final double value) {
    numericAtt.init(NumericUtils.doubleToSortableLong(value), valSize = 64, precisionStep, -precisionStep);
    return this;
  }
  
  /**
   * Initializes the token stream with the supplied <code>float</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new LegacyNumericTokenStream(precisionStep).setFloatValue(value))</code>
   */
  public LegacyNumericTokenStream setFloatValue(final float value) {
    numericAtt.init(NumericUtils.floatToSortableInt(value), valSize = 32, precisionStep, -precisionStep);
    return this;
  }
  
  @Override
  public void reset() {
    if (valSize == 0)
      throw new IllegalStateException("call set???Value() before usage");
    numericAtt.setShift(-precisionStep);
  }

  @Override
  public boolean incrementToken() {
    if (valSize == 0)
      throw new IllegalStateException("call set???Value() before usage");
    
    // this will only clear all other attributes in this TokenStream
    clearAttributes();

    final int shift = numericAtt.incShift();
    typeAtt.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC : TOKEN_TYPE_LOWER_PREC);
    posIncrAtt.setPositionIncrement((shift == 0) ? 1 : 0);
    return (shift < valSize);
  }

  /** Returns the precision step. */
  public int getPrecisionStep() {
    return precisionStep;
  }

  @Override
  public String toString() {
    // We override default because it can throw cryptic "illegal shift value":
    return getClass().getSimpleName() + "(precisionStep=" + precisionStep + " valueSize=" + numericAtt.getValueSize() + " shift=" + numericAtt.getShift() + ")";
  }
  
  // members
  private final LegacyNumericTermAttribute numericAtt = addAttribute(LegacyNumericTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  
  private int valSize = 0; // valSize==0 means not initialized
  private final int precisionStep;
}
