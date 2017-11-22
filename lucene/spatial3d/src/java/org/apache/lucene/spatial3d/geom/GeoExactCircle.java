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

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Circular area with a center and radius.
 *
 * @lucene.experimental
 */
class GeoExactCircle extends GeoBaseCircle {
  /** Center of circle */
  protected final GeoPoint center;
  /** Cutoff angle of circle (not quite the same thing as radius) */
  protected final double cutoffAngle;
  /** Actual accuracy */
  protected final double actualAccuracy;
  /** Planes describing the circle */
  protected final List<SidedPlane> circlePlanes;
  /** Bounds for the planes */
  protected final Map<Membership, Membership> eitherBounds;
  /** Back bounds for the planes */
  protected final Map<Membership, Membership> backBounds;
  /** A point that is on the world and on the circle plane */
  protected final GeoPoint[] edgePoints;
  /** The set of notable points for each edge */
  protected final List<GeoPoint[]> notableEdgePoints;
  /** Notable points for a circle -- there aren't any */
  protected static final GeoPoint[] circlePoints = new GeoPoint[0];

  /** Constructor.
   *@param planetModel is the planet model.
   *@param lat is the center latitude.
   *@param lon is the center longitude.
   *@param cutoffAngle is the surface radius for the circle.
   *@param accuracy is the allowed error value (linear distance).
   */
  public GeoExactCircle(final PlanetModel planetModel, final double lat, final double lon, final double cutoffAngle, final double accuracy) {
    super(planetModel);
    if (lat < -Math.PI * 0.5 || lat > Math.PI * 0.5)
      throw new IllegalArgumentException("Latitude out of bounds");
    if (lon < -Math.PI || lon > Math.PI)
      throw new IllegalArgumentException("Longitude out of bounds");
    if (cutoffAngle < 0.0)
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    if (cutoffAngle < Vector.MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Cutoff angle cannot be effectively zero");
    // We cannot allow exact circles to be large enough so that planes intersect at greater than 180 degrees.  This guarantees it.
    if (cutoffAngle > Math.PI * 0.5)
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    
    this.center = new GeoPoint(planetModel, lat, lon);
    this.cutoffAngle = cutoffAngle;

    if (accuracy < Vector.MINIMUM_RESOLUTION) {
      actualAccuracy = Vector.MINIMUM_RESOLUTION;
    } else {
      actualAccuracy = accuracy;
    }
    
    // We construct approximation planes until we have a low enough error estimate
    final List<ApproximationSlice> slices = new ArrayList<>(100);
    // Construct four cardinal points, and then we'll build the first two planes
    final GeoPoint northPoint = planetModel.surfacePointOnBearing(center, cutoffAngle, 0.0);
    final GeoPoint southPoint = planetModel.surfacePointOnBearing(center, cutoffAngle, Math.PI);
    final GeoPoint eastPoint = planetModel.surfacePointOnBearing(center, cutoffAngle, Math.PI * 0.5);
    final GeoPoint westPoint = planetModel.surfacePointOnBearing(center, cutoffAngle, Math.PI * 1.5);
    
    final GeoPoint edgePoint;
    if (planetModel.c > planetModel.ab) {
      // z can be greater than x or y, so ellipse is longer in height than width
      slices.add(new ApproximationSlice(center, eastPoint, Math.PI * 0.5, westPoint, Math.PI * -0.5, northPoint, 0.0));
      slices.add(new ApproximationSlice(center, westPoint, Math.PI * 1.5, eastPoint, Math.PI * 0.5, southPoint, Math.PI));
      edgePoint = eastPoint;
    } else {
      // z will be less than x or y, so ellipse is shorter than it is tall
      slices.add(new ApproximationSlice(center, northPoint, 0.0, southPoint, Math.PI, eastPoint, Math.PI * 0.5));
      slices.add(new ApproximationSlice(center, southPoint, Math.PI, northPoint, Math.PI * 2.0, westPoint, Math.PI * 1.5));
      edgePoint = northPoint;
    }
    //System.out.println("Edgepoint = " + edgePoint);
    
    final List<PlaneDescription> activeSlices = new ArrayList<>();
    
    // Now, iterate over slices until we have converted all of them into safe SidedPlanes.
    while (slices.size() > 0) {
      // Peel off a slice from the back
      final ApproximationSlice thisSlice = slices.remove(slices.size()-1);
      // Assess it to see if it is OK as it is, or needs to be split.
      // To do this, we need to look at the part of the circle that will have the greatest error.
      // We will need to compute bearing points for these.
      final double interpPoint1Bearing = (thisSlice.point1Bearing + thisSlice.middlePointBearing) * 0.5;
      final GeoPoint interpPoint1 = planetModel.surfacePointOnBearing(center, cutoffAngle, interpPoint1Bearing);
      final double interpPoint2Bearing = (thisSlice.point2Bearing + thisSlice.middlePointBearing) * 0.5;
      final GeoPoint interpPoint2 = planetModel.surfacePointOnBearing(center, cutoffAngle, interpPoint2Bearing);
      
      // Is this point on the plane? (that is, is the approximation good enough?)
      if (Math.abs(thisSlice.plane.evaluate(interpPoint1)) < actualAccuracy && Math.abs(thisSlice.plane.evaluate(interpPoint2)) < actualAccuracy) {
        if (activeSlices.size() == 0 || !activeSlices.get(activeSlices.size()-1).plane.isNumericallyIdentical(thisSlice.plane)) {
          activeSlices.add(new PlaneDescription(thisSlice.plane, thisSlice.endPoint1, thisSlice.endPoint2, thisSlice.middlePoint));
          //System.out.println("Point1 bearing = "+thisSlice.point1Bearing);
        } else if (activeSlices.size() > 0) {
          // Numerically identical plane; create a new slice to replace the one there.
          final PlaneDescription oldSlice = activeSlices.remove(activeSlices.size()-1);
          activeSlices.add(new PlaneDescription(thisSlice.plane, oldSlice.endPoint1, thisSlice.endPoint2, thisSlice.endPoint1));
          //System.out.println(" new endpoint2 bearing: "+thisSlice.point2Bearing);
        }
      } else {
        // Split the plane into two, and add it back to the end
        slices.add(new ApproximationSlice(center,
          thisSlice.endPoint1, thisSlice.point1Bearing, 
          thisSlice.middlePoint, thisSlice.middlePointBearing, 
          interpPoint1, interpPoint1Bearing));
        slices.add(new ApproximationSlice(center,
          thisSlice.middlePoint, thisSlice.middlePointBearing,
          thisSlice.endPoint2, thisSlice.point2Bearing,
          interpPoint2, interpPoint2Bearing));
      }
    }

    // Since the provide cutoff angle is really a surface distance, we need to use the point-on-bearing even for spheres.
    final List<SidedPlane> circlePlanes = new ArrayList<>(activeSlices.size());
    // If it turns out that there's only one circle plane, this array will be populated but unused
    final List<GeoPoint[]> notableEdgePoints = new ArrayList<>(activeSlices.size());
    // Back planes
    final Map<Membership, Membership> backPlanes = new HashMap<>(activeSlices.size());
    // Bounds
    final Map<Membership, Membership> bounds = new HashMap<>(activeSlices.size());
    
    // Compute bounding planes and actual circle planes
    for (int i = 0; i < activeSlices.size(); i++) {
      final PlaneDescription pd = activeSlices.get(i);
      // Calculate the backplane
      final Membership thisPlane = pd.plane;
      // Go back through all the earlier points until we find one that's not within
      GeoPoint backArticulationPoint = null;
      for (int j = 1; j < activeSlices.size(); j++) {
        int k = i - j;
        if (k < 0) {
          k += activeSlices.size();
        }
        final GeoPoint thisPoint = activeSlices.get(k).endPoint1;
        if (!thisPlane.isWithin(thisPoint)) {
          // Back up a notch
          k++;
          if (k >= activeSlices.size()) {
            k -= activeSlices.size();
          }
          backArticulationPoint = activeSlices.get(k).endPoint1;
          break;
        }
      }
      // Go forward until we find one that's not within
      GeoPoint forwardArticulationPoint = null;
      for (int j = 1; j < activeSlices.size(); j++) {
        int k = i + j;
        if (k >= activeSlices.size()) {
          k -= activeSlices.size();
        }
        final GeoPoint thisPoint = activeSlices.get(k).endPoint2;
        if (!thisPlane.isWithin(thisPoint)) {
          // back up
          k--;
          if (k < 0) {
            k += activeSlices.size();
          }
          forwardArticulationPoint = activeSlices.get(k).endPoint2;
          break;
        }
      }
      
      final Membership backPlane;
      if (backArticulationPoint != null && forwardArticulationPoint != null) {
        // We want a sided plane that goes through both identified articulation points and the center of the world.
        backPlane = new SidedPlane(pd.onSidePoint, true, backArticulationPoint, forwardArticulationPoint);
      } else {
        backPlane = null;
      }
      
      circlePlanes.add(pd.plane);
      if (backPlane != null) {
        backPlanes.put(pd.plane, backPlane);
      }
      notableEdgePoints.add(new GeoPoint[]{pd.endPoint1, pd.endPoint2});
      bounds.put(pd.plane, new EitherBound(new SidedPlane(pd.onSidePoint, pd.endPoint1, center), new SidedPlane(pd.onSidePoint, pd.endPoint2, center)));
    }

    //System.out.println("Number of planes needed: "+circlePlanes.size());
      
    this.circlePlanes = circlePlanes;
    // Compute bounds
    if (circlePlanes.size() == 1) {
      this.backBounds = null;
      this.eitherBounds = null;
      this.notableEdgePoints = null;
    } else {
      this.notableEdgePoints = notableEdgePoints;
      this.eitherBounds = bounds;
      this.backBounds = backPlanes;
    }
    
    this.edgePoints = new GeoPoint[]{edgePoint};      
    //System.out.println("Is edgepoint within? "+isWithin(edgePoint));
  }


  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoExactCircle(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, 
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream),
      SerializableObject.readDouble(inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, center.getLatitude());
    SerializableObject.writeDouble(outputStream, center.getLongitude());
    SerializableObject.writeDouble(outputStream, cutoffAngle);
    SerializableObject.writeDouble(outputStream, actualAccuracy);
  }

  @Override
  public double getRadius() {
    return cutoffAngle;
  }

  @Override
  public GeoPoint getCenter() {
    return center;
  }

  @Override
  protected double distance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    return distanceStyle.computeDistance(this.center, x, y, z);
  }

  @Override
  protected void distanceBounds(final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue) {
    // TBD: Compute actual bounds based on distance
    getBounds(bounds);
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (circlePlanes == null) {
      return 0.0;
    }
    if (circlePlanes.size() == 1) {
      return distanceStyle.computeDistance(planetModel, circlePlanes.get(0), x, y, z);
    }
    double outsideDistance = Double.POSITIVE_INFINITY;
    for (final SidedPlane plane : circlePlanes) {
      final double distance = distanceStyle.computeDistance(planetModel, plane, x, y, z, eitherBounds.get(plane));
      if (distance < outsideDistance) {
        outsideDistance = distance;
      }
    }
    return outsideDistance;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    if (circlePlanes == null) {
      return true;
    }
    for (final Membership plane : circlePlanes) {
      final Membership backPlane = (backBounds==null)?null:backBounds.get(plane);
      if (backPlane == null || backPlane.isWithin(x, y, z)) {
        if (!plane.isWithin(x, y, z)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    if (circlePlanes == null) {
      return false;
    }
    if (circlePlanes.size() == 1) {
      return circlePlanes.get(0).intersects(planetModel, p, notablePoints, circlePoints, bounds);
    }
    for (int edgeIndex = 0; edgeIndex < circlePlanes.size(); edgeIndex++) {
      final SidedPlane edge = circlePlanes.get(edgeIndex);
      final GeoPoint[] points = notableEdgePoints.get(edgeIndex);
      if (edge.intersects(planetModel, p, notablePoints, points, bounds, eitherBounds.get(edge))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean intersects(GeoShape geoShape) {
    if (circlePlanes == null) {
      return false;
    }
    if (circlePlanes.size() == 1) {
      return geoShape.intersects(circlePlanes.get(0), circlePoints);
    }
    for (int edgeIndex = 0; edgeIndex < circlePlanes.size(); edgeIndex++) {
      final SidedPlane edge = circlePlanes.get(edgeIndex);
      final GeoPoint[] points = notableEdgePoints.get(edgeIndex);
      if (geoShape.intersects(edge, points, eitherBounds.get(edge))) {
        return true;
      }
    }
    return false;
  }


  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    if (circlePlanes == null) {
      return;
    }
    bounds.addPoint(center);
    if (circlePlanes.size() == 1) {
      bounds.addPlane(planetModel, circlePlanes.get(0));
      return;
    }
    // Add bounds for all circle planes
    for (int edgeIndex = 0; edgeIndex < circlePlanes.size(); edgeIndex++) {
      final SidedPlane plane = circlePlanes.get(edgeIndex);
      bounds.addPlane(planetModel, plane, eitherBounds.get(plane));
      final GeoPoint[] points = notableEdgePoints.get(edgeIndex);
      for (final GeoPoint point : points) {
        bounds.addPoint(point);
      }
      // We don't bother to compute the intersection bounds since, unless the planet model is pathological, we expect planes to be intersecting at shallow
      // angles.
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoExactCircle))
      return false;
    GeoExactCircle other = (GeoExactCircle) o;
    return super.equals(other) && other.center.equals(center) && other.cutoffAngle == cutoffAngle && other.actualAccuracy == actualAccuracy;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + center.hashCode();
    long temp = Double.doubleToLongBits(cutoffAngle);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(actualAccuracy);
    result = 31 * result + (int) (temp ^ (temp >>> 32));    
    return result;
  }

  @Override
  public String toString() {
    return "GeoExactCircle: {planetmodel=" + planetModel+", center=" + center + ", radius=" + cutoffAngle + "(" + cutoffAngle * 180.0 / Math.PI + "), accuracy=" + actualAccuracy + "}";
  }
  
  /** A membership implementation representing edges that must apply.
   */
  protected static class EitherBound implements Membership {
    
    protected final Membership sideBound1;
    protected final Membership sideBound2;
    
    /** Constructor.
      * @param sideBound1 is the first side bound.
      * @param sideBound2 is the second side bound.
      */
    public EitherBound(final Membership sideBound1, final Membership sideBound2) {
      this.sideBound1 = sideBound1;
      this.sideBound2 = sideBound2;
    }

    @Override
    public boolean isWithin(final Vector v) {
      return sideBound1.isWithin(v) && sideBound2.isWithin(v);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return sideBound1.isWithin(x,y,z) && sideBound2.isWithin(x,y,z);
    }
    
    @Override
    public String toString() {
      return "(" + sideBound1 + "," + sideBound2 + ")";
    }
  }

  /** A temporary description of a plane that's part of an exact circle.
  */
  protected static class PlaneDescription {
    public final SidedPlane plane;
    public final GeoPoint endPoint1;
    public final GeoPoint endPoint2;
    public final GeoPoint onSidePoint;

    public PlaneDescription(final SidedPlane plane, final GeoPoint endPoint1, final GeoPoint endPoint2, final GeoPoint onSidePoint) {
      this.plane = plane;
      this.endPoint1 = endPoint1;
      this.endPoint2 = endPoint2;
      this.onSidePoint = onSidePoint;
    }
  }
  
  /** A temporary description of a section of circle.
   */
  protected static class ApproximationSlice {
    public final SidedPlane plane;
    public final GeoPoint endPoint1;
    public final double point1Bearing;
    public final GeoPoint endPoint2;
    public final double point2Bearing;
    public final GeoPoint middlePoint;
    public final double middlePointBearing;
    
    public ApproximationSlice(final GeoPoint center,
      final GeoPoint endPoint1, final double point1Bearing,
      final GeoPoint endPoint2, final double point2Bearing,
      final GeoPoint middlePoint, final double middlePointBearing) {
      this.endPoint1 = endPoint1;
      this.point1Bearing = point1Bearing;
      this.endPoint2 = endPoint2;
      this.point2Bearing = point2Bearing;
      this.middlePoint = middlePoint;
      this.middlePointBearing = middlePointBearing;
      // Construct the plane going through the three given points
      this.plane = SidedPlane.constructNormalizedThreePointSidedPlane(center, endPoint1, endPoint2, middlePoint);
      if (this.plane == null) {
        throw new IllegalArgumentException("Either circle is too large to fit on ellipsoid or accuracy is too high; could not construct a plane with endPoint1="+endPoint1+" bearing "+point1Bearing+", endPoint2="+endPoint2+" bearing "+point2Bearing+", middle="+middlePoint+" bearing "+middlePointBearing);
      }
      if (plane.isWithin(center) == false || !plane.evaluateIsZero(endPoint1) || !plane.evaluateIsZero(endPoint2) || !plane.evaluateIsZero(middlePoint))
        throw new IllegalStateException("SidedPlane constructor built a bad plane!!");
    }

    @Override
    public String toString() {
      return "{end point 1 = " + endPoint1 + " bearing 1 = "+point1Bearing + 
        " end point 2 = " + endPoint2 + " bearing 2 = " + point2Bearing + 
        " middle point = " + middlePoint + " middle bearing = " + middlePointBearing + "}";
    }

  }
  
  
}
