/**
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

package org.apache.lucene.spatial.geometry.shape;


/**
 * Common set of operations available on 2d shapes.
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public interface Geometry2D {
  /**
   * Translate according to the vector
   * @param v
   */
  public void translate(Vector2D v);
  
  /**
   * Does the shape contain the given point
   * @param p
   */
  public boolean contains(Point2D p);
  
  /**
   * Return the area
   */
  public double area();
  
  /**
   * Return the centroid
   */
  public Point2D centroid();
  
  /**
   * Returns information about how this shape intersects the given rectangle
   * @param r
   */
  public IntersectCase intersect(Rectangle r);

}
