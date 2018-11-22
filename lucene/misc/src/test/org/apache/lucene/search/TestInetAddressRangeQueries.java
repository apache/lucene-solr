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
package org.apache.lucene.search;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/**
 * Random testing for {@link InetAddressRange}
 */
@SuppressCodecs({"Direct"})
public class TestInetAddressRangeQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "ipRangeField";

  @Override
  protected Range nextRange(int dimensions) throws Exception {
    InetAddress min = nextInetaddress();
    byte[] bMin = InetAddressPoint.encode(min);
    InetAddress max = nextInetaddress();
    byte[] bMax = InetAddressPoint.encode(max);
    if (FutureArrays.compareUnsigned(bMin, 0, bMin.length, bMax, 0, bMin.length) > 0) {
      return new IpRange(max, min);
    }
    return new IpRange(min, max);
  }

  /** return random IPv4 or IPv6 address */
  private InetAddress nextInetaddress() throws UnknownHostException {
    byte[] b = random().nextBoolean() ? new byte[4] : new byte[16];
    switch (random().nextInt(5)) {
      case 0:
        return InetAddress.getByAddress(b);
      case 1:
        Arrays.fill(b, (byte) 0xff);
        return InetAddress.getByAddress(b);
      case 2:
        Arrays.fill(b, (byte) 42);
        return InetAddress.getByAddress(b);
      default:
        random().nextBytes(b);
        return InetAddress.getByAddress(b);
    }
  }

  @Override
  public void testRandomTiny() throws Exception {
    super.testRandomTiny();
  }

  @Override
  public void testMultiValued() throws Exception {
    super.testRandomMedium();
  }

  @Override
  public void testRandomMedium() throws Exception {
    super.testMultiValued();
  }

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    super.testRandomBig();
  }

  /** return random range */
  @Override
  protected InetAddressRange newRangeField(Range r) {
    return new InetAddressRange(FIELD_NAME, ((IpRange)r).minAddress, ((IpRange)r).maxAddress);
  }

  /** return random intersects query */
  @Override
  protected Query newIntersectsQuery(Range r) {
    return InetAddressRange.newIntersectsQuery(FIELD_NAME, ((IpRange)r).minAddress, ((IpRange)r).maxAddress);
  }

  /** return random contains query */
  @Override
  protected Query newContainsQuery(Range r) {
    return InetAddressRange.newContainsQuery(FIELD_NAME, ((IpRange)r).minAddress, ((IpRange)r).maxAddress);
  }

  /** return random within query */
  @Override
  protected Query newWithinQuery(Range r) {
    return InetAddressRange.newWithinQuery(FIELD_NAME, ((IpRange)r).minAddress, ((IpRange)r).maxAddress);
  }

  /** return random crosses query */
  @Override
  protected Query newCrossesQuery(Range r) {
    return InetAddressRange.newCrossesQuery(FIELD_NAME, ((IpRange)r).minAddress, ((IpRange)r).maxAddress);
  }

  /** encapsulated IpRange for test validation */
  private class IpRange extends Range {
    InetAddress minAddress;
    InetAddress maxAddress;
    byte[] min;
    byte[] max;

    IpRange(InetAddress min, InetAddress max) {
      this.minAddress = min;
      this.maxAddress = max;
      this.min = InetAddressPoint.encode(min);
      this.max = InetAddressPoint.encode(max);
    }

    @Override
    protected int numDimensions() {
      return 1;
    }

    @Override
    protected InetAddress getMin(int dim) {
      return minAddress;
    }

    @Override
    protected void setMin(int dim, Object val) {
      InetAddress v = (InetAddress)val;
      byte[] e = InetAddressPoint.encode(v);

      if (FutureArrays.compareUnsigned(min, 0, e.length, e, 0, e.length) < 0) {
        max = e;
        maxAddress = v;
      } else {
        min = e;
        minAddress = v;
      }
    }

    @Override
    protected InetAddress getMax(int dim) {
      return maxAddress;
    }

    @Override
    protected void setMax(int dim, Object val) {
      InetAddress v = (InetAddress)val;
      byte[] e = InetAddressPoint.encode(v);

      if (FutureArrays.compareUnsigned(max, 0, e.length, e, 0, e.length) > 0) {
        min = e;
        minAddress = v;
      } else {
        max = e;
        maxAddress = v;
      }
    }

    @Override
    protected boolean isEqual(Range o) {
      IpRange other = (IpRange)o;
      return Arrays.equals(min, other.min) && Arrays.equals(max, other.max);
    }

    @Override
    protected boolean isDisjoint(Range o) {
      IpRange other = (IpRange)o;
      return FutureArrays.compareUnsigned(min, 0, min.length, other.max, 0, min.length) > 0 ||
          FutureArrays.compareUnsigned(max, 0, max.length, other.min, 0, max.length) < 0;
    }

    @Override
    protected boolean isWithin(Range o) {
      IpRange other = (IpRange)o;
      return FutureArrays.compareUnsigned(min, 0, min.length, other.min, 0, min.length) >= 0 &&
          FutureArrays.compareUnsigned(max, 0, max.length, other.max, 0, max.length) <= 0;
    }

    @Override
    protected boolean contains(Range o) {
      IpRange other = (IpRange)o;
      return FutureArrays.compareUnsigned(min, 0, min.length, other.min, 0, min.length) <= 0 &&
          FutureArrays.compareUnsigned(max, 0, max.length, other.max, 0, max.length) >= 0;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("Box(");
      b.append(minAddress.getHostAddress());
      b.append(" TO ");
      b.append(maxAddress.getHostAddress());
      b.append(")");
      return b.toString();
    }
  }
}
