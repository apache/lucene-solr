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

import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.util.StringHelper;

/**
 * Random testing for {@link InetAddressRange}
 */
public class TestInetAddressRangeQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "ipRangeField";

  private IPVersion ipVersion;

  private enum IPVersion {IPv4, IPv6}

  @Override
  protected Range nextRange(int dimensions) throws Exception {
    InetAddress min = nextInetaddress();
    byte[] bMin = min.getAddress();
    InetAddress max = nextInetaddress();
    byte[] bMax = max.getAddress();
    if (StringHelper.compare(bMin.length, bMin, 0, bMax, 0) > 0) {
      return new IpRange(max, min);
    }
    return new IpRange(min, max);
  }

  /** return random IPv4 or IPv6 address */
  private InetAddress nextInetaddress() throws UnknownHostException {
    byte[] b;
    switch (ipVersion) {
      case IPv4:
        b = new byte[4];
        break;
      case IPv6:
        b = new byte[16];
        break;
      default:
        throw new IllegalArgumentException("incorrect IP version: " + ipVersion);
    }
    random().nextBytes(b);
    return InetAddress.getByAddress(b);
  }

  /** randomly select version across tests */
  private IPVersion ipVersion() {
    return random().nextBoolean() ? IPVersion.IPv4 : IPVersion.IPv6;
  }

  @Override
  public void testRandomTiny() throws Exception {
    ipVersion = ipVersion();
    super.testRandomTiny();
  }

  @Override
  public void testMultiValued() throws Exception {
    ipVersion = ipVersion();
    super.testRandomMedium();
  }

  @Override
  public void testRandomMedium() throws Exception {
    ipVersion = ipVersion();
    super.testMultiValued();
  }

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    ipVersion = ipVersion();
    super.testRandomBig();
  }

  /** return random range */
  @Override
  protected InetAddressRange newRangeField(Range r) {
    return new InetAddressRange(FIELD_NAME, ((IpRange)r).min, ((IpRange)r).max);
  }

  /** return random intersects query */
  @Override
  protected Query newIntersectsQuery(Range r) {
    return InetAddressRange.newIntersectsQuery(FIELD_NAME, ((IpRange)r).min, ((IpRange)r).max);
  }

  /** return random contains query */
  @Override
  protected Query newContainsQuery(Range r) {
    return InetAddressRange.newContainsQuery(FIELD_NAME, ((IpRange)r).min, ((IpRange)r).max);
  }

  /** return random within query */
  @Override
  protected Query newWithinQuery(Range r) {
    return InetAddressRange.newWithinQuery(FIELD_NAME, ((IpRange)r).min, ((IpRange)r).max);
  }

  /** return random crosses query */
  @Override
  protected Query newCrossesQuery(Range r) {
    return InetAddressRange.newCrossesQuery(FIELD_NAME, ((IpRange)r).min, ((IpRange)r).max);
  }

  /** encapsulated IpRange for test validation */
  private class IpRange extends Range {
    InetAddress min;
    InetAddress max;

    IpRange(InetAddress min, InetAddress max) {
      this.min = min;
      this.max = max;
    }

    @Override
    protected int numDimensions() {
      return 1;
    }

    @Override
    protected InetAddress getMin(int dim) {
      return min;
    }

    @Override
    protected void setMin(int dim, Object val) {
      byte[] v = ((InetAddress)val).getAddress();

      if (StringHelper.compare(v.length, min.getAddress(), 0, v, 0) < 0) {
        max = (InetAddress)val;
      } else {
        min = (InetAddress) val;
      }
    }

    @Override
    protected InetAddress getMax(int dim) {
      return max;
    }

    @Override
    protected void setMax(int dim, Object val) {
      byte[] v = ((InetAddress)val).getAddress();

      if (StringHelper.compare(v.length, max.getAddress(), 0, v, 0) > 0) {
        min = (InetAddress)val;
      } else {
        max = (InetAddress) val;
      }
    }

    @Override
    protected boolean isEqual(Range o) {
      IpRange other = (IpRange)o;
      return this.min.equals(other.min) && this.max.equals(other.max);
    }

    @Override
    protected boolean isDisjoint(Range o) {
      IpRange other = (IpRange)o;
      byte[] bMin = min.getAddress();
      byte[] bMax = max.getAddress();
      return StringHelper.compare(bMin.length, bMin, 0, other.max.getAddress(), 0) > 0 ||
          StringHelper.compare(bMax.length, bMax, 0, other.min.getAddress(), 0) < 0;
    }

    @Override
    protected boolean isWithin(Range o) {
      IpRange other = (IpRange)o;
      byte[] bMin = min.getAddress();
      byte[] bMax = max.getAddress();
      return StringHelper.compare(bMin.length, bMin, 0, other.min.getAddress(), 0) >= 0 &&
          StringHelper.compare(bMax.length, bMax, 0, other.max.getAddress(), 0) <= 0;
    }

    @Override
    protected boolean contains(Range o) {
      IpRange other = (IpRange)o;
      byte[] bMin = min.getAddress();
      byte[] bMax = max.getAddress();
      return StringHelper.compare(bMin.length, bMin, 0, other.min.getAddress(), 0) <= 0 &&
          StringHelper.compare(bMax.length, bMax, 0, other.max.getAddress(), 0) >= 0;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("Box(");
      b.append(min.getHostAddress());
      b.append(" TO ");
      b.append(max.getHostAddress());
      b.append(")");
      return b.toString();
    }
  }
}
