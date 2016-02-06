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
package org.apache.lucene.spatial.util;

/** Represents a lat/lon rectangle. */
public class GeoRect {
  public final double minLon;
  public final double maxLon;
  public final double minLat;
  public final double maxLat;

  public GeoRect(double minLon, double maxLon, double minLat, double maxLat) {
    if (GeoUtils.isValidLon(minLon) == false) {
      throw new IllegalArgumentException("invalid minLon " + minLon);
    }
    if (GeoUtils.isValidLon(maxLon) == false) {
      throw new IllegalArgumentException("invalid maxLon " + maxLon);
    }
    if (GeoUtils.isValidLat(minLat) == false) {
      throw new IllegalArgumentException("invalid minLat " + minLat);
    }
    if (GeoUtils.isValidLat(maxLat) == false) {
      throw new IllegalArgumentException("invalid maxLat " + maxLat);
    }
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
    assert maxLat >= minLat;

    // NOTE: cannot assert maxLon >= minLon since this rect could cross the dateline
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("GeoRect(lon=");
    b.append(minLon);
    b.append(" TO ");
    b.append(maxLon);
    if (maxLon < minLon) {
      b.append(" (crosses dateline!)");
    }
    b.append(" lat=");
    b.append(minLat);
    b.append(" TO ");
    b.append(maxLat);
    b.append(")");

    return b.toString();
  }
}
