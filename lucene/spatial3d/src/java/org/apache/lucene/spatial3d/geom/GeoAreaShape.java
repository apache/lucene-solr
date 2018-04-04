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
 * Shape that implements GeoArea. This type of shapes are able to resolve the
 * spatial relationship of other shapes with itself.
 *
 * @lucene.experimental
 */

public interface GeoAreaShape extends GeoMembershipShape, GeoArea{

  /**
   * Assess whether a shape intersects with any of the edges of this shape.
   * Note well that this method must return false if the shape contains or is disjoint
   * with the given shape.  It is permissible to return true if the shape is within the
   * specified shape, if it is difficult to compute intersection with edges.
   *
   * @param geoShape is the shape to assess for intersection with this shape's edges.
   *
   * @return true if there's such an intersection, false if not.
   */
  boolean intersects(GeoShape geoShape);
}
