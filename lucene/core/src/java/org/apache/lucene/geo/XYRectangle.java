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

/** Represents a x/y cartesian rectangle. */
public class XYRectangle {
  /** minimum x value */
  public final double minX;
  /** minimum y value */
  public final double maxX;
  /** maximum x value */
  public final double minY;
  /** maximum y value */
  public final double maxY;

  /** Constructs a bounding box by first validating the provided x and y coordinates */
  public XYRectangle(double minX, double maxX, double minY, double maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    assert minX <= maxX;
    assert minY <= maxY;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    XYRectangle rectangle = (XYRectangle) o;

    if (Double.compare(rectangle.minX, minX) != 0) return false;
    if (Double.compare(rectangle.minY, minY) != 0) return false;
    if (Double.compare(rectangle.maxX, maxX) != 0) return false;
    return Double.compare(rectangle.maxY, maxY) == 0;

  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(minX);
    result = (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minY);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxX);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxY);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("XYRectangle(x=");
    b.append(minX);
    b.append(" TO ");
    b.append(maxX);
    b.append(" y=");
    b.append(minY);
    b.append(" TO ");
    b.append(maxY);
    b.append(")");

    return b.toString();
  }
}
