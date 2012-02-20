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

package org.apache.lucene.spatial.base.shape;

/**
 * The set of spatial relationships.  Naming is consistent with OGC spec conventions as seen in SQL/MM and others.
 * No equality case.  If two Shape instances are equal then the result might be CONTAINS or WITHIN, and
 * some logic might fail under this edge condition when it's not careful to check.
 * Client code must be written to detect this and act accordingly.  In RectangleImpl.relate(), it checks
 * for this explicitly, for example.  TestShapes2D.assertRelation() checks too.
 */
public enum SpatialRelation {
  WITHIN,
  CONTAINS,
  DISJOINT,
  INTERSECTS;
  //Don't have these: TOUCHES, CROSSES, OVERLAPS

  public SpatialRelation transpose() {
    switch(this) {
      case CONTAINS: return SpatialRelation.WITHIN;
      case WITHIN: return SpatialRelation.CONTAINS;
      default: return this;
    }
  }

  /**
   * If you were to call aShape.relate(bShape) and aShape.relate(cShape), you could call
   * this to merge the intersect results as if bShape & cShape were combined into {@link MultiShape}.
   * @param other
   * @return
   */
  public SpatialRelation combine(SpatialRelation other) {
    if (this == other)
      return this;
    if (this == WITHIN || other == WITHIN)
      return WITHIN;
    return INTERSECTS;
  }

  public boolean intersects() {
    return this != DISJOINT;
  }

  /** Not commutative!  WITHIN.inverse().inverse() != WITHIN. */
  public SpatialRelation inverse() {
    switch(this) {
      case DISJOINT: return CONTAINS;
      case CONTAINS: return DISJOINT;
      case WITHIN: return INTERSECTS;//not commutative!
    }
    return INTERSECTS;
  }
}
