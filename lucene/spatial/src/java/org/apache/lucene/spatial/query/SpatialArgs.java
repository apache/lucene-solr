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

import java.util.Locale;

import com.spatial4j.core.shape.Shape;

/**
 * Principally holds the query {@link Shape} and the {@link SpatialOperation}.
 *
 * @lucene.experimental
 */
public class SpatialArgs {

  public static final double DEFAULT_DIST_PRECISION = 0.025d;

  private SpatialOperation operation;
  private Shape shape;
  private double distPrecision = DEFAULT_DIST_PRECISION;

  public SpatialArgs(SpatialOperation operation) {
    this.operation = operation;
  }

  public SpatialArgs(SpatialOperation operation, Shape shape) {
    this.operation = operation;
    this.shape = shape;
  }

  /** Check if the arguments make sense -- throw an exception if not */
  public void validate() throws IllegalArgumentException {
    if (operation.isTargetNeedsArea() && !shape.hasArea()) {
      throw new IllegalArgumentException(operation + " only supports geometry with area");
    }
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append(operation.getName()).append('(');
    str.append(shape.toString());
    str.append(" distPrec=").append(String.format(Locale.ROOT, "%.2f%%", distPrecision / 100d));
    str.append(')');
    return str.toString();
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

  /** Considers {@link SpatialOperation#BBoxWithin} in returning the shape. */
  public Shape getShape() {
    if (shape != null && (operation == SpatialOperation.BBoxWithin || operation == SpatialOperation.BBoxIntersects))
      return shape.getBoundingBox();
    return shape;
  }

  public void setShape(Shape shape) {
    this.shape = shape;
  }

  /**
   * A measure of acceptable error of the shape.  It is specified as the
   * fraction of the distance from the center of the query shape to its furthest
   * bounding box corner.  This effectively inflates the size of the shape but
   * should not shrink it.
   * <p/>
   * The default is {@link #DEFAULT_DIST_PRECISION}
   *
   * @return 0 to 0.5
   */
  public Double getDistPrecision() {
    return distPrecision;
  }

  public void setDistPrecision(Double distPrecision) {
    if (distPrecision != null)
      this.distPrecision = distPrecision;
  }

}
