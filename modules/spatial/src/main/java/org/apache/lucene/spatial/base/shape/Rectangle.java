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

import org.apache.lucene.spatial.base.context.SpatialContext;

public interface Rectangle extends Shape {

  public double getWidth();
  public double getHeight();

  public double getMinX();
  public double getMinY();
  public double getMaxX();
  public double getMaxY();

  /** If {@link #hasArea()} then this returns the area, otherwise it returns 0. */
  public double getArea();
  /** Only meaningful for geospatial contexts. */
  public boolean getCrossesDateLine();

  /* There is no axis line shape, and this is more efficient then creating a flat Rectangle for intersect(). */
  public SpatialRelation relate_yRange(double minY, double maxY, SpatialContext ctx);
  public SpatialRelation relate_xRange(double minX, double maxX, SpatialContext ctx);
}
