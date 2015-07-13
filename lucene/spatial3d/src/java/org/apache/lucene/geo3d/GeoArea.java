package org.apache.lucene.geo3d;

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

/**
 * A GeoArea represents a standard 2-D breakdown of a part of sphere.  It can
 * be bounded in latitude, or bounded in both latitude and longitude, or not
 * bounded at all.  The purpose of the interface is to describe bounding shapes used for
 * computation of geo hashes.
 *
 * @lucene.experimental
 */
public interface GeoArea extends Membership {
  // Since we don't know what each GeoArea's constraints are,
  // we put the onus on the GeoArea implementation to do the right thing.
  // This will, of course, rely heavily on methods provided by
  // the underlying GeoShape class.

  // Relationship values for "getRelationship()"
  
  /** The referenced shape CONTAINS this shape */
  public static final int CONTAINS = 0;
  /** The referenced shape IS WITHIN this shape */
  public static final int WITHIN = 1;
  /** The referenced shape OVERLAPS this shape */
  public static final int OVERLAPS = 2;
  /** The referenced shape has no relation to this shape */
  public static final int DISJOINT = 3;

  /**
   * Find the spatial relationship between a shape and the current geo area.
   * Note: return value is how the GeoShape relates to the GeoArea, not the
   * other way around. For example, if this GeoArea is entirely within the
   * shape, then CONTAINS should be returned.  If the shape is entirely enclosed
   * by this GeoArea, then WITHIN should be returned.
   * Note well: When a shape consists of multiple independent overlapping subshapes,
   * it is sometimes impossible to determine the distinction between
   * OVERLAPS and CONTAINS.  In that case, OVERLAPS may be returned even
   * though the proper result would in fact be CONTAINS.  Code accordingly.
   *
   * @param shape is the shape to consider.
   * @return the relationship, from the perspective of the shape.
   */
  public int getRelationship(GeoShape shape);
}

