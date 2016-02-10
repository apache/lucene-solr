package org.apache.lucene.spatial.geopoint.document;

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

import java.util.Objects;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.spatial.util.GeoEncodingUtils;

import static org.apache.lucene.spatial.geopoint.document.GeoPointField.PRECISION_STEP;

/**
 * <b>Expert:</b> This class provides a {@link TokenStream} used by {@link GeoPointField}
 * for encoding {@link GeoPointField.TermEncoding#PREFIX} only GeoPointTerms.
 *
 * <p><i>NOTE: This is used as the default encoding unless
 * {@code GeoPointField.setNumericType(FieldType.LegacyNumericType.LONG)} is set</i></p>
 *
 * This class is similar to {@link org.apache.lucene.analysis.NumericTokenStream} but encodes terms up to a
 * a maximum of {@link #MAX_SHIFT} using a fixed precision step defined by
 * {@link GeoPointField#PRECISION_STEP}. This yields a total of 4 terms per GeoPoint
 * each consisting of 5 bytes (4 prefix bytes + 1 precision byte).
 *
 * <p>For best performance use the provided {@link GeoPointField#PREFIX_TYPE_NOT_STORED} or
 * {@link GeoPointField#PREFIX_TYPE_STORED}</p>
 *
 * <p>If prefix terms are used then the default GeoPoint query constructors may be used, but if
 * {@link org.apache.lucene.analysis.NumericTokenStream} is used, then be sure to pass
 * {@link GeoPointField.TermEncoding#NUMERIC} to all GeoPointQuery constructors</p>
 *
 * Here's an example usage:
 *
 * <pre class="prettyprint">
 *   // using prefix terms
 *   GeoPointField geoPointField = new GeoPointField(fieldName1, lon, lat, GeoPointField.PREFIX_TYPE_NOT_STORED);
 *   document.add(geoPointField);
 *
 *   // query by bounding box (default uses TermEncoding.PREFIX)
 *   Query q = new GeoPointInBBoxQuery(fieldName1, minLon, minLat, maxLon, maxLat);
 *
 *   // using numeric terms
 *   geoPointField = new GeoPointField(fieldName2, lon, lat, GeoPointField.NUMERIC_TYPE_NOT_STORED);
 *   document.add(geoPointField);
 *
 *   // query by distance (requires TermEncoding.NUMERIC)
 *   q = new GeoPointDistanceQuery(fieldName2, TermEncoding.NUMERIC, centerLon, centerLat, radiusMeters);
 * </pre>
 *
 * @lucene.experimental
 */
final class GeoPointTokenStream extends TokenStream {
  private static final int MAX_SHIFT = PRECISION_STEP * 4;

  private final GeoPointTermAttribute geoPointTermAtt = addAttribute(GeoPointTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  private boolean isInit = false;

  /**
   * Expert: Creates a token stream for geo point fields with the specified
   * <code>precisionStep</code> using the given
   * {@link org.apache.lucene.util.AttributeFactory}.
   * The stream is not yet initialized,
   * before using set a value using the various setGeoCode method.
   */
  public GeoPointTokenStream() {
    super(new GeoPointAttributeFactory(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY));
    assert PRECISION_STEP > 0;
  }

  public GeoPointTokenStream setGeoCode(final long geoCode) {
    geoPointTermAtt.init(geoCode, MAX_SHIFT-PRECISION_STEP);
    isInit = true;
    return this;
  }

  @Override
  public void reset() {
    if (isInit == false) {
      throw new IllegalStateException("call setGeoCode() before usage");
    }
  }

  @Override
  public boolean incrementToken() {
    if (isInit == false) {
      throw new IllegalStateException("call setGeoCode() before usage");
    }

    // this will only clear all other attributes in this TokenStream
    clearAttributes();

    final int shift = geoPointTermAtt.incShift();
    posIncrAtt.setPositionIncrement((shift == MAX_SHIFT) ? 1 : 0);
    return (shift < 63);
  }

  /**
   * Tracks shift values during encoding
   */
  public interface GeoPointTermAttribute extends Attribute {
    /** Returns current shift value, undefined before first token */
    int getShift();

    /** <em>Don't call this method!</em>
     * @lucene.internal */
    void init(long value, int shift);

    /** <em>Don't call this method!</em>
     * @lucene.internal */
    int incShift();
  }

  // just a wrapper to prevent adding CTA
  private static final class GeoPointAttributeFactory extends AttributeFactory {
    private final AttributeFactory delegate;

    GeoPointAttributeFactory(AttributeFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
      if (CharTermAttribute.class.isAssignableFrom(attClass)) {
        throw new IllegalArgumentException("GeoPointTokenStream does not support CharTermAttribute.");
      }
      return delegate.createAttributeInstance(attClass);
    }
  }

  public static final class GeoPointTermAttributeImpl extends AttributeImpl implements GeoPointTermAttribute,TermToBytesRefAttribute {
    private long value = 0L;
    private int shift = 0;
    private BytesRefBuilder bytes = new BytesRefBuilder();

    public GeoPointTermAttributeImpl() {
      this.shift = MAX_SHIFT-PRECISION_STEP;
    }

    @Override
    public BytesRef getBytesRef() {
      GeoEncodingUtils.geoCodedToPrefixCoded(value, shift, bytes);
      return bytes.get();
    }

    @Override
    public void init(long value, int shift) {
      this.value = value;
      this.shift = shift;
    }

    @Override
    public int getShift() { return shift; }

    @Override
    public int incShift() {
      return (shift += PRECISION_STEP);
    }

    @Override
    public void clear() {
      // this attribute has no contents to clear!
      // we keep it untouched as it's fully controlled by outer class.
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(TermToBytesRefAttribute.class, "bytes", getBytesRef());
      reflector.reflect(GeoPointTermAttribute.class, "shift", shift);
    }

    @Override
    public void copyTo(AttributeImpl target) {
      final GeoPointTermAttribute a = (GeoPointTermAttribute) target;
      a.init(value, shift);
    }

    @Override
    public GeoPointTermAttributeImpl clone() {
      GeoPointTermAttributeImpl t = (GeoPointTermAttributeImpl)super.clone();
      // Do a deep clone
      t.bytes = new BytesRefBuilder();
      t.bytes.copyBytes(getBytesRef());
      return t;
    }

    @Override
    public int hashCode() {
      return Objects.hash(shift, value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      GeoPointTermAttributeImpl other = (GeoPointTermAttributeImpl) obj;
      if (shift != other.shift) return false;
      if (value != other.value) return false;
      return true;
    }
  }

  /** override toString because it can throw cryptic "illegal shift value": */
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(precisionStep=" + PRECISION_STEP + " shift=" + geoPointTermAtt.getShift() + ")";
  }
}