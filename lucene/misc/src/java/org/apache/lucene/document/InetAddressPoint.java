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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.NumericUtils;

/** 
 * An indexed 128-bit {@code InetAddress} field.
 * <p>
 * Finding all documents within a range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery(String, InetAddress)} for matching an exact network address.
 *   <li>{@link #newPrefixQuery(String, InetAddress, int)} for matching a network based on CIDR prefix.
 *   <li>{@link #newRangeQuery(String, InetAddress, InetAddress)} for matching arbitrary network address ranges.
 *   <li>{@link #newSetQuery(String, InetAddress...)} for matching a set of network addresses.
 * </ul>
 * <p>
 * This field supports both IPv4 and IPv6 addresses: IPv4 addresses are converted
 * to <a href="https://tools.ietf.org/html/rfc4291#section-2.5.5">IPv4-Mapped IPv6 Addresses</a>:
 * indexing {@code 1.2.3.4} is the same as indexing {@code ::FFFF:1.2.3.4}.
 * @see PointValues
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

  /** The minimum value that an ip address can hold. */
  public static final InetAddress MIN_VALUE;
  /** The maximum value that an ip address can hold. */
  public static final InetAddress MAX_VALUE;
  static {
    MIN_VALUE = decode(new byte[BYTES]);
    byte[] maxValueBytes = new byte[BYTES];
    Arrays.fill(maxValueBytes, (byte) 0xFF);
    MAX_VALUE = decode(maxValueBytes);
  }

  /**
   * Return the {@link InetAddress} that compares immediately greater than
   * {@code address}.
   * @throws ArithmeticException if the provided address is the
   *              {@link #MAX_VALUE maximum ip address}
   */
  public static InetAddress nextUp(InetAddress address) {
    if (address.equals(MAX_VALUE)) {
      throw new ArithmeticException("Overflow: there is no greater InetAddress than "
          + address.getHostAddress());
    }
    byte[] delta = new byte[BYTES];
    delta[BYTES-1] = 1;
    byte[] nextUpBytes = new byte[InetAddressPoint.BYTES];
    NumericUtils.add(InetAddressPoint.BYTES, 0, encode(address), delta, nextUpBytes);
    return decode(nextUpBytes);
  }

  /**
   * Return the {@link InetAddress} that compares immediately less than
   * {@code address}.
   * @throws ArithmeticException if the provided address is the
   *              {@link #MIN_VALUE minimum ip address}
   */
  public static InetAddress nextDown(InetAddress address) {
    if (address.equals(MIN_VALUE)) {
      throw new ArithmeticException("Underflow: there is no smaller InetAddress than "
          + address.getHostAddress());
    }
    byte[] delta = new byte[BYTES];
    delta[BYTES-1] = 1;
    byte[] nextDownBytes = new byte[InetAddressPoint.BYTES];
    NumericUtils.subtract(InetAddressPoint.BYTES, 0, encode(address), delta, nextDownBytes);
    return decode(nextDownBytes);
  }

  /** Change the values of this field */
  public void setInetAddressValue(InetAddress value) {
    if (value == null) {
      throw new IllegalArgumentException("point must not be null");
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
    return newRangeQuery(field, value, value);
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
    if (value == null) {
      throw new IllegalArgumentException("InetAddress must not be null");
    }
    if (prefixLength < 0 || prefixLength > 8 * value.getAddress().length) {
      throw new IllegalArgumentException("illegal prefixLength '" + prefixLength + "'. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges");
    }
    // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
    byte lower[] = value.getAddress();
    byte upper[] = value.getAddress();
    for (int i = prefixLength; i < 8 * lower.length; i++) {
      int m = 1 << (7 - (i & 7));
      lower[i >> 3] &= ~m;
      upper[i >> 3] |= m;
    }
    try {
      return newRangeQuery(field, InetAddress.getByAddress(lower), InetAddress.getByAddress(upper));
    } catch (UnknownHostException e) {
      throw new AssertionError(e); // values are coming from InetAddress
    }
  }

  /** 
   * Create a range query for network addresses.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = InetAddressPoint.MIN_VALUE} or
   * {@code upperValue = InetAddressPoint.MAX_VALUE}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@code InetAddressPoint#nextUp(lowerValue)}
   * or {@code InetAddressPoint#nexDown(upperValue)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be null.
   * @param upperValue upper portion of the range (inclusive). must not be null.
   * @throws IllegalArgumentException if {@code field} is null, {@code lowerValue} is null, 
   *                                  or {@code upperValue} is null
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, InetAddress lowerValue, InetAddress upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, encode(lowerValue), encode(upperValue), 1) {
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
   * @param values all values to match
   */
  public static Query newSetQuery(String field, InetAddress... values) {

    // We must compare the encoded form (InetAddress doesn't implement Comparable, and even if it
    // did, we do our own thing with ipv4 addresses):

    // NOTE: we could instead convert-per-comparison and save this extra array, at cost of slower sort:
    byte[][] sortedValues = new byte[values.length][];
    for(int i=0;i<values.length;i++) {
      sortedValues[i] = encode(values[i]);
    }

    Arrays.sort(sortedValues,
                new Comparator<byte[]>() {
                  @Override
                  public int compare(byte[] a, byte[] b) {
                    return FutureArrays.compareUnsigned(a, 0, BYTES, b, 0, BYTES);
                  }
                });

    final BytesRef encoded = new BytesRef(new byte[BYTES]);

    return new PointInSetQuery(field, 1, BYTES,
                               new PointInSetQuery.Stream() {

                                 int upto;

                                 @Override
                                 public BytesRef next() {
                                   if (upto == sortedValues.length) {
                                     return null;
                                   } else {
                                     encoded.bytes = sortedValues[upto];
                                     assert encoded.bytes.length == encoded.length;
                                     upto++;
                                     return encoded;
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
