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

import static org.apache.lucene.geo.XYEncodingUtils.decode;
import static org.apache.lucene.geo.XYEncodingUtils.encode;

/**
 * 2D rectangle implementation containing cartesian spatial logic.
 *
 * @lucene.internal
 */
public class XYRectangle2D extends Rectangle2D {

  protected XYRectangle2D(double minX, double maxX, double minY, double maxY) {
    super(encode(minX), encode(maxX), encode(minY), encode(maxY));
  }

  /** Builds a Rectangle2D from rectangle */
  public static XYRectangle2D create(XYRectangle rectangle) {
    return new XYRectangle2D(rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY);
  }

  @Override
  public boolean crossesDateline() {
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("XYRectangle(x=");
    sb.append(decode(minX));
    sb.append(" TO ");
    sb.append(decode(maxX));
    sb.append(" y=");
    sb.append(decode(minY));
    sb.append(" TO ");
    sb.append(decode(maxY));
    sb.append(")");
    return sb.toString();
  }
}
