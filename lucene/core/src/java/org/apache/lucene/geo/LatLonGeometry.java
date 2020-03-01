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

/**
 * Lat/Lon Geometry object.
 */
public abstract class LatLonGeometry {

  /** get a {@link Component2D} from this geometry */
  protected abstract Component2D toComponent2D();

  /** Creates a Component2D from the provided LatLonGeometry array */
  public static Component2D create(LatLonGeometry... latLonGeometries) {
    if (latLonGeometries == null) {
      throw new IllegalArgumentException("geometries must not be null");
    }
    if (latLonGeometries.length == 0) {
      throw new IllegalArgumentException("geometries must not be empty");
    }
    if (latLonGeometries.length == 1) {
      if (latLonGeometries[0] == null) {
        throw new IllegalArgumentException("geometries[0] must not be null");
      }
      return latLonGeometries[0].toComponent2D();
    }
    Component2D[] components = new Component2D[latLonGeometries.length];
    for (int i = 0; i < latLonGeometries.length; i++) {
      if (latLonGeometries[i] == null) {
        throw new IllegalArgumentException("geometries[" + i + "] must not be null");
      }
      components[i] = latLonGeometries[i].toComponent2D();
    }
    return ComponentTree.create(components);
  }
}
