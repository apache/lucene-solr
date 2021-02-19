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
package org.apache.lucene.spatial3d.geom;

/**
 * Factory for {@link XYZSolid}.
 *
 * @lucene.experimental
 */
public class XYZSolidFactory {
  private XYZSolidFactory() {}

  /**
   * Create a XYZSolid of the right kind given (x,y,z) bounds.
   *
   * @param planetModel is the planet model
   * @param minX is the min X boundary
   * @param maxX is the max X boundary
   * @param minY is the min Y boundary
   * @param maxY is the max Y boundary
   * @param minZ is the min Z boundary
   * @param maxZ is the max Z boundary
   */
  public static XYZSolid makeXYZSolid(
      final PlanetModel planetModel,
      final double minX,
      final double maxX,
      final double minY,
      final double maxY,
      final double minZ,
      final double maxZ) {
    if (Math.abs(maxX - minX) < Vector.MINIMUM_RESOLUTION) {
      if (Math.abs(maxY - minY) < Vector.MINIMUM_RESOLUTION) {
        if (Math.abs(maxZ - minZ) < Vector.MINIMUM_RESOLUTION) {
          return new dXdYdZSolid(planetModel, (minX + maxX) * 0.5, (minY + maxY) * 0.5, minZ);
        } else {
          return new dXdYZSolid(planetModel, (minX + maxX) * 0.5, (minY + maxY) * 0.5, minZ, maxZ);
        }
      } else {
        if (Math.abs(maxZ - minZ) < Vector.MINIMUM_RESOLUTION) {
          return new dXYdZSolid(planetModel, (minX + maxX) * 0.5, minY, maxY, (minZ + maxZ) * 0.5);
        } else {
          return new dXYZSolid(planetModel, (minX + maxX) * 0.5, minY, maxY, minZ, maxZ);
        }
      }
    }
    if (Math.abs(maxY - minY) < Vector.MINIMUM_RESOLUTION) {
      if (Math.abs(maxZ - minZ) < Vector.MINIMUM_RESOLUTION) {
        return new XdYdZSolid(planetModel, minX, maxX, (minY + maxY) * 0.5, (minZ + maxZ) * 0.5);
      } else {
        return new XdYZSolid(planetModel, minX, maxX, (minY + maxY) * 0.5, minZ, maxZ);
      }
    }
    if (Math.abs(maxZ - minZ) < Vector.MINIMUM_RESOLUTION) {
      return new XYdZSolid(planetModel, minX, maxX, minY, maxY, (minZ + maxZ) * 0.5);
    }
    return new StandardXYZSolid(planetModel, minX, maxX, minY, maxY, minZ, maxZ);
  }

  /**
   * Create a XYZSolid of the right kind given (x,y,z) bounds.
   *
   * @param planetModel is the planet model
   * @param bounds is the XYZ bounds object.
   * @return the solid.
   */
  public static XYZSolid makeXYZSolid(final PlanetModel planetModel, final XYZBounds bounds) {
    return makeXYZSolid(
        planetModel,
        bounds.getMinimumX(),
        bounds.getMaximumX(),
        bounds.getMinimumY(),
        bounds.getMaximumY(),
        bounds.getMinimumZ(),
        bounds.getMaximumZ());
  }
}
