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

/**
 * Reusable geo-relation utility methods
 */
public class GeoRelationUtils {

  // No instance:
  private GeoRelationUtils() {
  }

  /**
   * Determine if a bbox (defined by minLat, maxLat, minLon, maxLon) contains the provided point (defined by lat, lon)
   * NOTE: this is a basic method that does not handle dateline or pole crossing. Unwrapping must be done before
   * calling this method.
   */
  public static boolean pointInRectPrecise(final double lat, final double lon,
                                           final double minLat, final double maxLat,
                                           final double minLon, final double maxLon) {
    return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
  }

  /////////////////////////
  // Rectangle relations
  /////////////////////////

  /**
   * Computes whether two rectangles are disjoint
   */
  private static boolean rectDisjoint(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                      final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return (aMaxLon < bMinLon || aMinLon > bMaxLon || aMaxLat < bMinLat || aMinLat > bMaxLat);
  }

  /**
   * Computes whether the first (a) rectangle is wholly within another (b) rectangle (shared boundaries allowed)
   */
  public static boolean rectWithin(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                   final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return !(aMinLon < bMinLon || aMinLat < bMinLat || aMaxLon > bMaxLon || aMaxLat > bMaxLat);
  }

  /**
   * Computes whether two rectangles cross
   */
  public static boolean rectCrosses(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                    final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return !(rectDisjoint(aMinLat, aMaxLat, aMinLon, aMaxLon, bMinLat, bMaxLat, bMinLon, bMaxLon) ||
             rectWithin(aMinLat, aMaxLat, aMinLon, aMaxLon, bMinLat, bMaxLat, bMinLon, bMaxLon));
  }

  /**
   * Computes whether a rectangle intersects another rectangle (crosses, within, touching, etc)
   */
  public static boolean rectIntersects(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                       final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return !((aMaxLon < bMinLon || aMinLon > bMaxLon || aMaxLat < bMinLat || aMinLat > bMaxLat));
  }
}
