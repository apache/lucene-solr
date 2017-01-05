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
package org.apache.lucene.spatial3d.geom;

/**
 * Implemented by Geo3D shapes that can calculate if a point is within it or not.
 *
 * @lucene.experimental
 */
public interface Membership {

  /**
   * Check if a point is within this shape.
   *
   * @param point is the point to check.
   * @return true if the point is within this shape
   */
  public default boolean isWithin(final Vector point) {
    return isWithin(point.x, point.y, point.z);
  }

  /**
   * Check if a point is within this shape.
   *
   * @param x is x coordinate of point to check.
   * @param y is y coordinate of point to check.
   * @param z is z coordinate of point to check.
   * @return true if the point is within this shape
   */
  public boolean isWithin(final double x, final double y, final double z);

}
