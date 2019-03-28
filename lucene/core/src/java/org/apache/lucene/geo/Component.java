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

package org.apache.lucene.geo;

import org.apache.lucene.index.PointValues;

/**
 * Defines the spatial relations a component needs to support
 */
public interface Component {
  /** relates this component with a bounding box**/
  PointValues.Relation relate(double minY, double maxY, double minX, double maxX);
  /** relates this component with a triangle**/
  PointValues.Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy);
  /** relates this component with a point**/
  boolean contains(double lat, double lon);
  /** bounding box for this component **/
  Rectangle getBoundingBox();
}
