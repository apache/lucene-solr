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

/** Cartesian Geometry object. */
public abstract class XYGeometry {

  /** get a Component2D from this object */
  protected abstract Component2D toComponent2D();

  /** Creates a Component2D from the provided XYGeometries array */
  public static Component2D create(XYGeometry... xyGeometries) {
    if (xyGeometries == null) {
      throw new IllegalArgumentException("geometries must not be null");
    }
    if (xyGeometries.length == 0) {
      throw new IllegalArgumentException("geometries must not be empty");
    }
    if (xyGeometries.length == 1) {
      if (xyGeometries[0] == null) {
        throw new IllegalArgumentException("geometries[0] must not be null");
      }
      return xyGeometries[0].toComponent2D();
    }
    Component2D[] components = new Component2D[xyGeometries.length];
    for (int i = 0; i < xyGeometries.length; i++) {
      if (xyGeometries[i] == null) {
        throw new IllegalArgumentException("geometries[" + i + "] must not be null");
      }
      components[i] = xyGeometries[i].toComponent2D();
    }
    return ComponentTree.create(components);
  }
}
