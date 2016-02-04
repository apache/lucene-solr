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
package org.apache.lucene.geo3d;

/**
 * An object for accumulating XYZ bounds information.
 *
 * @lucene.experimental
 */
public class XYZBounds implements Bounds {

  /** A 'fudge factor', which is added to maximums and subtracted from minimums,
   * in order to compensate for potential error deltas.  This would not be necessary
   * except that our 'bounds' is defined as always equaling or exceeding the boundary
   * of the shape, and we cannot guarantee that without making MINIMUM_RESOLUTION
   * unacceptably large.
   */
  protected static final double FUDGE_FACTOR = Vector.MINIMUM_RESOLUTION * 2.0;
  
  /** Minimum x */
  protected Double minX = null;
  /** Maximum x */
  protected Double maxX = null;
  /** Minimum y */
  protected Double minY = null;
  /** Maximum y */
  protected Double maxY = null;
  /** Minimum z */
  protected Double minZ = null;
  /** Maximum z */
  protected Double maxZ = null;
  
  /** Set to true if no longitude bounds can be stated */
  protected boolean noLongitudeBound = false;
  /** Set to true if no top latitude bound can be stated */
  protected boolean noTopLatitudeBound = false;
  /** Set to true if no bottom latitude bound can be stated */
  protected boolean noBottomLatitudeBound = false;

  /** Construct an empty bounds object */
  public XYZBounds() {
  }

  // Accessor methods
  
  /** Return the minimum X value.
   *@return minimum X value.
   */
  public Double getMinimumX() {
    return minX;
  }

  /** Return the maximum X value.
   *@return maximum X value.
   */
  public Double getMaximumX() {
    return maxX;
  }

  /** Return the minimum Y value.
   *@return minimum Y value.
   */
  public Double getMinimumY() {
    return minY;
  }

  /** Return the maximum Y value.
   *@return maximum Y value.
   */
  public Double getMaximumY() {
    return maxY;
  }
  
  /** Return the minimum Z value.
   *@return minimum Z value.
   */
  public Double getMinimumZ() {
    return minZ;
  }

  /** Return the maximum Z value.
   *@return maximum Z value.
   */
  public Double getMaximumZ() {
    return maxZ;
  }

  /** Return true if minX is as small as the planet model allows.
   *@return true if minX has reached its bound.
   */
  public boolean isSmallestMinX(final PlanetModel planetModel) {
    if (minX == null)
      return false;
    return minX - planetModel.getMinimumXValue() < Vector.MINIMUM_RESOLUTION;
  }
  
  /** Return true if maxX is as large as the planet model allows.
   *@return true if maxX has reached its bound.
   */
  public boolean isLargestMaxX(final PlanetModel planetModel) {
    if (maxX == null)
      return false;
    return planetModel.getMaximumXValue() - maxX < Vector.MINIMUM_RESOLUTION;
  }

  /** Return true if minY is as small as the planet model allows.
   *@return true if minY has reached its bound.
   */
  public boolean isSmallestMinY(final PlanetModel planetModel) {
    if (minY == null)
      return false;
    return minY - planetModel.getMinimumYValue() < Vector.MINIMUM_RESOLUTION;
  }
  
  /** Return true if maxY is as large as the planet model allows.
   *@return true if maxY has reached its bound.
   */
  public boolean isLargestMaxY(final PlanetModel planetModel) {
    if (maxY == null)
      return false;
    return planetModel.getMaximumYValue() - maxY < Vector.MINIMUM_RESOLUTION;
  }
  
  /** Return true if minZ is as small as the planet model allows.
   *@return true if minZ has reached its bound.
   */
  public boolean isSmallestMinZ(final PlanetModel planetModel) {
    if (minZ == null)
      return false;
    return minZ - planetModel.getMinimumZValue() < Vector.MINIMUM_RESOLUTION;
  }
  
  /** Return true if maxZ is as large as the planet model allows.
   *@return true if maxZ has reached its bound.
   */
  public boolean isLargestMaxZ(final PlanetModel planetModel) {
    if (maxZ == null)
      return false;
    return planetModel.getMaximumZValue() - maxZ < Vector.MINIMUM_RESOLUTION;
  }

  // Modification methods
  
  @Override
  public Bounds addPlane(final PlanetModel planetModel, final Plane plane, final Membership... bounds) {
    plane.recordBounds(planetModel, this, bounds);
    return this;
  }

  /** Add a horizontal plane to the bounds description.
   * This method should EITHER use the supplied latitude, OR use the supplied
   * plane, depending on what is most efficient.
   *@param planetModel is the planet model.
   *@param latitude is the latitude.
   *@param horizontalPlane is the plane.
   *@param bounds are the constraints on the plane.
   *@return updated Bounds object.
   */
  public Bounds addHorizontalPlane(final PlanetModel planetModel,
    final double latitude,
    final Plane horizontalPlane,
    final Membership... bounds) {
    return addPlane(planetModel, horizontalPlane, bounds);
  }
    
  /** Add a vertical plane to the bounds description.
   * This method should EITHER use the supplied longitude, OR use the supplied
   * plane, depending on what is most efficient.
   *@param planetModel is the planet model.
   *@param longitude is the longitude.
   *@param verticalPlane is the plane.
   *@param bounds are the constraints on the plane.
   *@return updated Bounds object.
   */
  public Bounds addVerticalPlane(final PlanetModel planetModel,
    final double longitude,
    final Plane verticalPlane,
    final Membership... bounds) {
    return addPlane(planetModel, verticalPlane, bounds);
  }

  @Override
  public Bounds addXValue(final GeoPoint point) {
    final double x = point.x;
    final double small = x - FUDGE_FACTOR;
    if (minX == null || minX > small) {
      minX = new Double(small);
    }
    final double large = x + FUDGE_FACTOR;
    if (maxX == null || maxX < large) {
      maxX = new Double(large);
    }
    return this;
  }

  @Override
  public Bounds addYValue(final GeoPoint point) {
    final double y = point.y;
    final double small = y - FUDGE_FACTOR;
    if (minY == null || minY > small) {
      minY = new Double(small);
    }
    final double large = y + FUDGE_FACTOR;
    if (maxY == null || maxY < large) {
      maxY = new Double(large);
    }
    return this;
  }

  @Override
  public Bounds addZValue(final GeoPoint point) {
    final double z = point.z;
    final double small = z - FUDGE_FACTOR;
    if (minZ == null || minZ > small) {
      minZ = new Double(small);
    }
    final double large = z + FUDGE_FACTOR;
    if (maxZ == null || maxZ < large) {
      maxZ = new Double(large);
    }
    return this;
  }

  @Override
  public Bounds addPoint(final GeoPoint point) {
    return addXValue(point).addYValue(point).addZValue(point);
  }

  @Override
  public Bounds isWide() {
    // No specific thing we need to do.
    return this;
  }

  @Override
  public Bounds noLongitudeBound() {
    // No specific thing we need to do.
    return this;
  }

  @Override
  public Bounds noTopLatitudeBound() {
    // No specific thing we need to do.
    return this;
  }

  @Override
  public Bounds noBottomLatitudeBound() {
    // No specific thing we need to do.
    return this;
  }

}
