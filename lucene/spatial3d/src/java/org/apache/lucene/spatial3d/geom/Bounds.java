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
 * An interface for accumulating bounds information.
 * The bounds object is initially empty.  Bounding points
 * are then applied by supplying (x,y,z) tuples.  It is also
 * possible to indicate the following edge cases:
 * (1) No longitude bound possible
 * (2) No upper latitude bound possible
 * (3) No lower latitude bound possible
 * When any of these have been applied, further application of
 * points cannot override that decision.
 *
 * @lucene.experimental
 */
public interface Bounds {

  /** Add a general plane to the bounds description.
   *@param planetModel is the planet model.
   *@param plane is the plane.
   *@param bounds are the membership bounds for points along the arc.
   */
  public Bounds addPlane(final PlanetModel planetModel, final Plane plane, final Membership... bounds);
  
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
    final Membership... bounds);
    
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
    final Membership... bounds);

  /** Add the intersection between two planes to the bounds description.
   * Where the shape has intersecting planes, it is better to use this method
   * than just adding the point, since this method takes each plane's error envelope into
   * account.
   *@param planetModel is the planet model.
   *@param plane1 is the first plane.
   *@param plane2 is the second plane.
   *@param bounds are the membership bounds for the intersection.
   */
  public Bounds addIntersection(final PlanetModel planetModel, final Plane plane1, final Plane plane2, final Membership... bounds);

  /** Add a single point.
   *@param point is the point.
   *@return the updated Bounds object.
   */
  public Bounds addPoint(final GeoPoint point);

  /** Add an X value.
   *@param point is the point to take the x value from.
   *@return the updated object.
   */
  public Bounds addXValue(final GeoPoint point);

  /** Add a Y value.
   *@param point is the point to take the y value from.
   *@return the updated object.
   */
  public Bounds addYValue(final GeoPoint point);

  /** Add a Z value.
   *@param point is the point to take the z value from.
   *@return the updated object.
   */
  public Bounds addZValue(final GeoPoint point);
  
  /** Signal that the shape exceeds Math.PI in longitude.
   *@return the updated Bounds object.
   */
  public Bounds isWide();
  
  /** Signal that there is no longitude bound.
   *@return the updated Bounds object.
   */
  public Bounds noLongitudeBound();

  /** Signal that there is no top latitude bound.
   *@return the updated Bounds object.
   */
  public Bounds noTopLatitudeBound();

  /** Signal that there is no bottom latitude bound.
   *@return the updated Bounds object.
   */
  public Bounds noBottomLatitudeBound();
  
  /** Signal that there is no bound whatsoever.
   * The bound is limited only by the constraints of the
   * planet.
   *@return the updated Bounds object.,
   */
  public Bounds noBound(final PlanetModel planetModel);
  
}
