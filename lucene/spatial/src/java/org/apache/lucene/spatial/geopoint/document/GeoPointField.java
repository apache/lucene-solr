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
package org.apache.lucene.spatial.geopoint.document;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import static org.apache.lucene.spatial.util.MortonEncoder.encode;
import static org.apache.lucene.geo.GeoUtils.MIN_LAT_INCL;
import static org.apache.lucene.geo.GeoUtils.MIN_LON_INCL;

/**
 * <p>
 * Field that indexes <code>latitude</code> <code>longitude</code> decimal-degree values
 * for efficient encoding, sorting, and querying. This Geo capability is intended
 * to provide a basic and efficient out of the box field type for indexing and
 * querying 2 dimensional points in WGS-84 decimal degrees. An example usage is as follows:
 *
 * <pre class="prettyprint">
 *  document.add(new GeoPointField(name, -96.33, 32.66, Field.Store.NO));
 * </pre>
 *
 * <p>To perform simple geospatial queries against a <code>GeoPointField</code>,
 * see {@link org.apache.lucene.spatial.geopoint.search.GeoPointInBBoxQuery}, {@link org.apache.lucene.spatial.geopoint.search.GeoPointInPolygonQuery},
 * or {@link org.apache.lucene.spatial.geopoint.search.GeoPointDistanceQuery}
 *
 * @lucene.experimental
 */
public final class GeoPointField extends Field {
  /** encoding step value for GeoPoint prefix terms */
  public static final int PRECISION_STEP = 9;

  /** number of bits used for quantizing latitude and longitude values */
  public static final short BITS = 31;
  /** scaling factors to convert lat/lon into unsigned space */
  private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;
  private static final double LON_SCALE = (0x1L<<BITS)/360.0D;

  /**
   * The maximum term length (used for <code>byte[]</code> buffer size)
   * for encoding <code>geoEncoded</code> values.
   * @see #geoCodedToPrefixCodedBytes(long, int, BytesRefBuilder)
   */
  private static final int BUF_SIZE_LONG = 28/8 + 1;

  /**
   * Type for a GeoPointField that is not stored:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_NOT_STORED = new FieldType();
  static {
    TYPE_NOT_STORED.setTokenized(false);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_NOT_STORED.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE_NOT_STORED.freeze();
  }

  /**
   * Type for a stored GeoPointField:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_STORED = new FieldType();
  static {
    TYPE_STORED.setTokenized(false);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_STORED.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE_STORED.setStored(true);
    TYPE_STORED.freeze();
  }

  /** Creates a stored or un-stored GeoPointField
   *  @param name field name
   *  @param latitude latitude double value [-90.0 : 90.0]
   *  @param longitude longitude double value [-180.0 : 180.0]
   *  @param stored Store.YES if the content should also be stored
   *  @throws IllegalArgumentException if the field name is null.
   */
  public GeoPointField(String name, double latitude, double longitude, Store stored) {
    this(name, latitude, longitude, getFieldType(stored));
  }

  /** Expert: allows you to customize the {@link
   *  FieldType}.
   *  @param name field name
   *  @param latitude latitude double value [-90.0 : 90.0]
   *  @param longitude longitude double value [-180.0 : 180.0]
   *  @param type customized field type
   *  @throws IllegalArgumentException if the field name or type is null
   */
  public GeoPointField(String name, double latitude, double longitude, FieldType type) {
    super(name, type);

    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);

    // field must be indexed
    // todo does it make sense here to provide the ability to store a GeoPointField but not index?
    if (type.indexOptions() == IndexOptions.NONE && type.stored() == false) {
      throw new IllegalArgumentException("type.indexOptions() is set to NONE but type.stored() is false");
    } else if (type.indexOptions() == IndexOptions.DOCS) {
      if (type.docValuesType() != DocValuesType.SORTED_NUMERIC) {
        throw new IllegalArgumentException("type.docValuesType() must be SORTED_NUMERIC but got " + type.docValuesType());
      }
    } else {
      throw new IllegalArgumentException("type.indexOptions() must be one of NONE or DOCS but got " + type.indexOptions());
    }

    // set field data
    fieldsData = encodeLatLon(latitude, longitude);
  }

  /**
   * Static helper method for returning a valid FieldType based on termEncoding and stored options
   */
  private static FieldType getFieldType(Store stored) {
    if (stored == Store.YES) {
      return TYPE_STORED;
    } else if (stored == Store.NO) {
      return TYPE_NOT_STORED;
    } else {
      throw new IllegalArgumentException("stored option must be NO or YES but got " + stored);
    }
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    if (fieldType().indexOptions() == IndexOptions.NONE) {
      // not indexed
      return null;
    }

    if (reuse instanceof GeoPointTokenStream == false) {
      reuse = new GeoPointTokenStream();
    }

    final GeoPointTokenStream gpts = (GeoPointTokenStream)reuse;
    gpts.setGeoCode(((Number) fieldsData).longValue());

    return reuse;
  }

  /** access latitude value */
  public double getLat() {
    return decodeLatitude((long) fieldsData);
  }

  /** access longitude value */
  public double getLon() {
    return decodeLongitude((long) fieldsData);
  }

  @Override
  public String toString() {
    if (fieldsData == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    sb.append(decodeLatitude((long) fieldsData));
    sb.append(',');
    sb.append(decodeLongitude((long) fieldsData));
    return sb.toString();
  }

  /*************************
   * 31 bit encoding utils *
   *************************/
  public static long encodeLatLon(final double lat, final double lon) {
    long result = encode(lat, lon);
    if (result == 0xFFFFFFFFFFFFFFFFL) {
      return result & 0xC000000000000000L;
    }
    return result >>> 2;
  }

  /** decode longitude value from morton encoded geo point */
  public static final double decodeLongitude(final long hash) {
    return unscaleLon(BitUtil.deinterleave(hash));
  }

  /** decode latitude value from morton encoded geo point */
  public static final double decodeLatitude(final long hash) {
    return unscaleLat(BitUtil.deinterleave(hash >>> 1));
  }

  private static final double unscaleLon(final long val) {
    return (val / LON_SCALE) + MIN_LON_INCL;
  }

  private static final double unscaleLat(final long val) {
    return (val / LAT_SCALE) + MIN_LAT_INCL;
  }

  /** Convert a geocoded morton long into a prefix coded geo term */
  public static void geoCodedToPrefixCoded(long hash, int shift, BytesRefBuilder bytes) {
    geoCodedToPrefixCodedBytes(hash, shift, bytes);
  }

  /** Convert a prefix coded geo term back into the geocoded morton long */
  public static long prefixCodedToGeoCoded(final BytesRef val) {
    final long result = 0L
        | (val.bytes[val.offset+0] & 255L) << 24
        | (val.bytes[val.offset+1] & 255L) << 16
        | (val.bytes[val.offset+2] & 255L) << 8
        | val.bytes[val.offset+3] & 255L;

    return result << 32;
  }

  /**
   * GeoTerms are coded using 4 prefix bytes + 1 byte to record number of prefix bits
   *
   * example prefix at shift 54 (yields 10 significant prefix bits):
   *  pppppppp pp000000 00000000 00000000 00001010
   *  (byte 1) (byte 2) (byte 3) (byte 4) (sigbits)
   */
  private static void geoCodedToPrefixCodedBytes(final long hash, final int shift, final BytesRefBuilder bytes) {
    // ensure shift is 32..63
    if (shift < 32 || shift > 63) {
      throw new IllegalArgumentException("Illegal shift value, must be 32..63; got shift=" + shift);
    }
    int nChars = BUF_SIZE_LONG + 1; // one extra for the byte that contains the number of significant bits
    bytes.setLength(nChars);
    bytes.grow(nChars--);
    final int sigBits = 64 - shift;
    bytes.setByteAt(BUF_SIZE_LONG, (byte)(sigBits));
    long sortableBits = hash;
    sortableBits >>>= shift;
    sortableBits <<= 32 - sigBits;
    do {
      bytes.setByteAt(--nChars, (byte)(sortableBits));
      sortableBits >>>= 8;
    } while (nChars > 0);
  }

  /** Get the prefix coded geo term shift value */
  public static int getPrefixCodedShift(final BytesRef val) {
    final int shift = val.bytes[val.offset + BUF_SIZE_LONG];
    if (shift > 63 || shift < 0)
      throw new NumberFormatException("Invalid shift value (" + shift + ") in prefixCoded bytes (is encoded value really a geo point?)");
    return shift;
  }
}
