package org.apache.lucene.spatial.query;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;

/**
 * Principally holds the query {@link Shape} and the {@link SpatialOperation}.
 * It's used as an argument to some methods on {@link org.apache.lucene.spatial.SpatialStrategy}.
 *
 * @lucene.experimental
 */
public class SpatialArgs {

  public static final double DEFAULT_DISTERRPCT = 0.025d;

  private SpatialOperation operation;
  private Shape shape;
  private Double distErrPct;
  private Double distErr;

  public SpatialArgs(SpatialOperation operation, Shape shape) {
    if (operation == null || shape == null)
      throw new NullPointerException("operation and shape are required");
    this.operation = operation;
    this.shape = shape;
  }

  /**
   * Computes the distance given a shape and the {@code distErrPct}.  The
   * algorithm is the fraction of the distance from the center of the query
   * shape to its closest bounding box corner.
   *
   * @param shape Mandatory.
   * @param distErrPct 0 to 0.5
   * @param ctx Mandatory
   * @return A distance (in degrees).
   */
  public static double calcDistanceFromErrPct(Shape shape, double distErrPct, SpatialContext ctx) {
    if (distErrPct < 0 || distErrPct > 0.5) {
      throw new IllegalArgumentException("distErrPct " + distErrPct + " must be between [0 to 0.5]");
    }
    if (distErrPct == 0 || shape instanceof Point) {
      return 0;
    }
    Rectangle bbox = shape.getBoundingBox();
    //Compute the distance from the center to a corner.  Because the distance
    // to a bottom corner vs a top corner can vary in a geospatial scenario,
    // take the closest one (greater precision).
    Point ctr = bbox.getCenter();
    double y = (ctr.getY() >= 0 ? bbox.getMaxY() : bbox.getMinY());
    double diagonalDist = ctx.getDistCalc().distance(ctr, bbox.getMaxX(), y);
    return diagonalDist * distErrPct;
  }

  /**
   * Gets the error distance that specifies how precise the query shape is. This
   * looks at {@link #getDistErr()}, {@link #getDistErrPct()}, and {@code
   * defaultDistErrPct}.
   * @param defaultDistErrPct 0 to 0.5
   * @return >= 0
   */
  public double resolveDistErr(SpatialContext ctx, double defaultDistErrPct) {
    if (distErr != null)
      return distErr;
    double distErrPct = (this.distErrPct != null ? this.distErrPct : defaultDistErrPct);
    return calcDistanceFromErrPct(shape, distErrPct, ctx);
  }

  /** Check if the arguments make sense -- throw an exception if not */
  public void validate() throws IllegalArgumentException {
    if (operation.isTargetNeedsArea() && !shape.hasArea()) {
      throw new IllegalArgumentException(operation + " only supports geometry with area");
    }
    if (distErr != null && distErrPct != null)
      throw new IllegalArgumentException("Only distErr or distErrPct can be specified.");
  }

  @Override
  public String toString() {
    return SpatialArgsParser.writeSpatialArgs(this);
  }

  //------------------------------------------------
  // Getters & Setters
  //------------------------------------------------

  public SpatialOperation getOperation() {
    return operation;
  }

  public void setOperation(SpatialOperation operation) {
    this.operation = operation;
  }

  public Shape getShape() {
    return shape;
  }

  public void setShape(Shape shape) {
    this.shape = shape;
  }

  /**
   * A measure of acceptable error of the shape as a fraction.  This effectively
   * inflates the size of the shape but should not shrink it.
   *
   * @return 0 to 0.5
   * @see #calcDistanceFromErrPct(com.spatial4j.core.shape.Shape, double,
   *      com.spatial4j.core.context.SpatialContext)
   */
  public Double getDistErrPct() {
    return distErrPct;
  }

  public void setDistErrPct(Double distErrPct) {
    if (distErrPct != null)
      this.distErrPct = distErrPct;
  }

  /**
   * The acceptable error of the shape.  This effectively inflates the
   * size of the shape but should not shrink it.
   *
   * @return >= 0
   */
  public Double getDistErr() {
    return distErr;
  }

  public void setDistErr(Double distErr) {
    this.distErr = distErr;
  }
}
