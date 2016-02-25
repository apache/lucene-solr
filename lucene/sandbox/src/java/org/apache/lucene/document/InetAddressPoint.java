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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/** 
 * An indexed 128-bit {@code InetAddress} field.
 * <p>
 * Finding all documents within a range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery newExactQuery()} for matching an exact network address.
 *   <li>{@link #newPrefixQuery newPrefixQuery()} for matching a network based on CIDR prefix.
 *   <li>{@link #newRangeQuery newRangeQuery()} for matching arbitrary network address ranges.
 *   <li>{@link #newSetQuery newSetQuery()} for matching a set of 1D values.
 * </ul>
 * <p>
 * This field supports both IPv4 and IPv6 addresses: IPv4 addresses are converted
 * to <a href="https://tools.ietf.org/html/rfc4291#section-2.5.5">IPv4-Mapped IPv6 Addresses</a>:
 * indexing {@code 1.2.3.4} is the same as indexing {@code ::FFFF:1.2.3.4}.
 */
public class InetAddressPoint extends Field {

  // implementation note: we convert all addresses to IPv6: we expect prefix compression of values,
  // so its not wasteful, but allows one field to handle both IPv4 and IPv6.
  /** The number of bytes per dimension: 128 bits */
  public static final int BYTES = 16;
  
  // rfc4291 prefix
  static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 }; 

  private static final FieldType TYPE;
  static {
    TYPE = new FieldType();
    TYPE.setDimensions(1, BYTES);
    TYPE.freeze();
  }

  /** Change the values of this field */
  public void setInetAddressValue(InetAddress value) {
    if (value == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    fieldsData = new BytesRef(encode(value));
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from InetAddress to BytesRef");
  }

  /** Creates a new InetAddressPoint, indexing the
   *  provided address.
   *
   *  @param name field name
   *  @param point InetAddress value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public InetAddressPoint(String name, InetAddress point) {
    super(name, TYPE);
    setInetAddressValue(point);
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    // IPv6 addresses are bracketed, to not cause confusion with historic field:value representation
    BytesRef bytes = (BytesRef) fieldsData;
    InetAddress address = decode(BytesRef.deepCopyOf(bytes).bytes);
    if (address.getAddress().length == 16) {
      result.append('[');
      result.append(address.getHostAddress());
      result.append(']');
    } else {
      result.append(address.getHostAddress());
    }

    result.append('>');
    return result.toString();
  }
  
  // public helper methods (e.g. for queries)

  /** Encode InetAddress value into binary encoding */
  public static byte[] encode(InetAddress value) {
    byte[] address = value.getAddress();
    if (address.length == 4) {
      byte[] mapped = new byte[16];
      System.arraycopy(IPV4_PREFIX, 0, mapped, 0, IPV4_PREFIX.length);
      System.arraycopy(address, 0, mapped, IPV4_PREFIX.length, address.length);
      address = mapped;
    } else if (address.length != 16) {
      // more of an assertion, how did you create such an InetAddress :)
      throw new UnsupportedOperationException("Only IPv4 and IPv6 addresses are supported");
    }
    return address;
  }
  
  /** Decodes InetAddress value from binary encoding */
  public static InetAddress decode(byte value[]) {
    try {
      return InetAddress.getByAddress(value);
    } catch (UnknownHostException e) {
      // this only happens if value.length != 4 or 16, strange exception class
      throw new IllegalArgumentException("encoded bytes are of incorrect length", e);
    }
  }

  // static methods for generating queries

  /** 
   * Create a query for matching a network address.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, InetAddress value) {
    return newRangeQuery(field, value, true, value, true);
  }
  
  /** 
   * Create a prefix query for matching a CIDR network range.
   *
   * @param field field name. must not be {@code null}.
   * @param value any host address
   * @param prefixLength the network prefix length for this address. This is also known as the subnet mask in the context of IPv4 addresses.
   * @throws IllegalArgumentException if {@code field} is null, or prefixLength is invalid.
   * @return a query matching documents with addresses contained within this network
   */
  public static Query newPrefixQuery(String field, InetAddress value, int prefixLength) {
    if (prefixLength < 0 || prefixLength > 8 * value.getAddress().length) {
      throw new IllegalArgumentException("illegal prefixLength '" + prefixLength + "'. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges");
    }
    // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
    byte lower[] = value.getAddress();
    byte upper[] = value.getAddress();
    for (int i = prefixLength; i < 8 * lower.length; i++) {
      lower[i >> 3] &= ~(1 << (i & 7));
      upper[i >> 3] |= 1 << (i & 7);
    }
    try {
      return newRangeQuery(field, InetAddress.getByAddress(lower), true, InetAddress.getByAddress(upper), true);
    } catch (UnknownHostException e) {
      throw new AssertionError(e); // values are coming from InetAddress
    }
  }

  /** 
   * Create a range query for network addresses.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, InetAddress lowerValue, boolean lowerInclusive, InetAddress upperValue, boolean upperInclusive) {
    byte[][] lowerBytes = new byte[1][];
    if (lowerValue != null) {
      lowerBytes[0] = encode(lowerValue);
    }
    byte[][] upperBytes = new byte[1][];
    if (upperValue != null) {
      upperBytes[0] = encode(upperValue);
    }
    return new PointRangeQuery(field, lowerBytes, new boolean[] { lowerInclusive }, upperBytes, new boolean[] { upperInclusive }) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return decode(value).getHostAddress(); // for ranges, the range itself is already bracketed
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param valuesIn all values to match
   */
  public static Query newSetQuery(String field, InetAddress... valuesIn) throws IOException {

    // Don't unexpectedly change the user's incoming values array:
    InetAddress[] values = valuesIn.clone();

    Arrays.sort(values);

    final BytesRef value = new BytesRef(new byte[BYTES]);

    return new PointInSetQuery(field, 1, BYTES,
                               new BytesRefIterator() {

                                 int upto;

                                 @Override
                                 public BytesRef next() {
                                   if (upto == values.length) {
                                     return null;
                                   } else {
                                     value.bytes = encode(values[upto]);
                                     assert value.bytes.length == value.length;
                                     upto++;
                                     return value;
                                   }
                                 }
                               }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == BYTES;
        return decode(value).getHostAddress();
      }
    };
  }
}
