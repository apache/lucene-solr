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

package org.apache.lucene.spatial.base.shape.simple;

import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.shape.SpatialRelation;
import org.apache.lucene.spatial.base.shape.Point;
import org.apache.lucene.spatial.base.shape.Rectangle;


public class GeoCircleImpl extends CircleImpl {
  private final double distDEG;// [0 TO 180]
  private final GeoCircleImpl inverseCircle;//when distance reaches > 1/2 way around the world, cache the inverse.
  private final double horizAxisY;//see getYAxis

  public GeoCircleImpl(Point p, double dist, SpatialContext ctx) {
    super(p, dist, ctx);
    assert ctx.isGeo();

    //In the direction of latitude (N,S), distance is the same number of degrees.
    distDEG = ctx.getDistCalc().distanceToDegrees(distance);

    if (distDEG > 90) {
      assert enclosingBox.getWidth() == 360;
      double backDistDEG = 180 - distDEG;
      if (backDistDEG >= 0) {
        double backDistance = ctx.getDistCalc().degreesToDistance(backDistDEG);
        Point backPoint = ctx.makePoint(getCenter().getX() + 180, getCenter().getY() + 180);
        inverseCircle = new GeoCircleImpl(backPoint,backDistance,ctx);
      } else
        inverseCircle = null;//whole globe
      horizAxisY = getCenter().getY();//although probably not used
    } else {
      inverseCircle = null;
      double _horizAxisY = ctx.getDistCalc().calcBoxByDistFromPtHorizAxis(getCenter(), distance, ctx);
      //some rare numeric conditioning cases can cause this to be barely beyond the box
      if (_horizAxisY > enclosingBox.getMaxY()) {
        horizAxisY = enclosingBox.getMaxY();
      } else if (_horizAxisY < enclosingBox.getMinY()) {
        horizAxisY = enclosingBox.getMinY();
      } else {
        horizAxisY = _horizAxisY;
      }
      //assert enclosingBox.relate_yRange(horizAxis,horizAxis,ctx).intersects();
    }

  }

  @Override
  protected double getYAxis() {
    return horizAxisY;
  }

  /**
   * Called after bounding box is intersected.
   * @bboxSect INTERSECTS or CONTAINS from enclosingBox's intersection
   * @result DISJOINT, CONTAINS, or INTERSECTS (not WITHIN)
   */
  @Override
  protected SpatialRelation relateRectanglePhase2(Rectangle r, SpatialRelation bboxSect, SpatialContext ctx) {

    //Rectangle wraps around the world longitudinally creating a solid band; there are no corners to test intersection
    if (r.getWidth() == 360) {
      return SpatialRelation.INTERSECTS;
    }

    if (inverseCircle != null) {
      return inverseCircle.relate(r, ctx).inverse();
    }

    //if a pole is wrapped, we have a separate algorithm
    if (enclosingBox.getWidth() == 360) {
      return relateRectangleCircleWrapsPole(r, ctx);
    }

    //This is an optimization path for when there are no dateline or pole issues.
    if (!enclosingBox.getCrossesDateLine() && !r.getCrossesDateLine()) {
      return super.relateRectanglePhase2(r, bboxSect, ctx);
    }

    //do quick check to see if all corners are within this circle for CONTAINS
    int cornersIntersect = numCornersIntersect(r);
    if (cornersIntersect == 4) {
      //ensure r's x axis is within c's.  If it doesn't, r sneaks around the globe to touch the other side (intersect).
      SpatialRelation xIntersect = r.relate_xRange(enclosingBox.getMinX(), enclosingBox.getMaxX(), ctx);
      if (xIntersect == SpatialRelation.WITHIN)
        return SpatialRelation.CONTAINS;
      return SpatialRelation.INTERSECTS;
    }

    //INTERSECT or DISJOINT ?
    if (cornersIntersect > 0)
      return SpatialRelation.INTERSECTS;

    //Now we check if one of the axis of the circle intersect with r.  If so we have
    // intersection.

    /* x axis intersects  */
    if ( r.relate_yRange(getYAxis(), getYAxis(), ctx).intersects() // at y vertical
          && r.relate_xRange(enclosingBox.getMinX(), enclosingBox.getMaxX(), ctx).intersects() )
      return SpatialRelation.INTERSECTS;

    /* y axis intersects */
    if (r.relate_xRange(getXAxis(), getXAxis(), ctx).intersects()) { // at x horizontal
      double yTop = getCenter().getY()+ distDEG;
      assert yTop <= 90;
      double yBot = getCenter().getY()- distDEG;
      assert yBot >= -90;
      if (r.relate_yRange(yBot, yTop, ctx).intersects())//back bottom
        return SpatialRelation.INTERSECTS;
    }

    return SpatialRelation.DISJOINT;
  }

  private SpatialRelation relateRectangleCircleWrapsPole(Rectangle r, SpatialContext ctx) {
    //This method handles the case where the circle wraps ONE pole, but not both.  For both,
    // there is the inverseCircle case handled before now.  The only exception is for the case where
    // the circle covers the entire globe, and we'll check that first.
    if (distDEG == 180)//whole globe
      return SpatialRelation.CONTAINS;

    //Check if r is within the pole wrap region:
    double yTop = getCenter().getY()+ distDEG;
    if (yTop > 90) {
      double yTopOverlap = yTop - 90;
      assert yTopOverlap <= 90;
      if (r.getMinY() >= 90 - yTopOverlap)
        return SpatialRelation.CONTAINS;
    } else {
      double yBot = point.getY() - distDEG;
      if (yBot < -90) {
        double yBotOverlap = -90 - yBot;
        assert yBotOverlap <= 90;
        if (r.getMaxY() <= -90 + yBotOverlap)
          return SpatialRelation.CONTAINS;
      } else {
        //This point is probably not reachable ??
        assert yTop == 90 || yBot == -90;//we simply touch a pole
        //continue
      }
    }

    //If there are no corners to check intersection because r wraps completely...
    if (r.getWidth() == 360)
      return SpatialRelation.INTERSECTS;

    //Check corners:
    int cornersIntersect = numCornersIntersect(r);
    // (It might be possible to reduce contains() calls within nCI() to exactly two, but this intersection
    //  code is complicated enough as it is.)
    if (cornersIntersect == 4) {//all
      double backX = ctx.normX(getCenter().getX()+180);
      if (r.relate_xRange(backX, backX, ctx).intersects())
        return SpatialRelation.INTERSECTS;
      else
        return SpatialRelation.CONTAINS;
    } else if (cornersIntersect == 0) {//none
      double frontX = getCenter().getX();
      if (r.relate_xRange(frontX, frontX, ctx).intersects())
        return SpatialRelation.INTERSECTS;
      else
        return SpatialRelation.DISJOINT;
    } else//partial
      return SpatialRelation.INTERSECTS;
  }

  /** Returns either 0 for none, 1 for some, or 4 for all. */
  private int numCornersIntersect(Rectangle r) {
    //We play some logic games to avoid calling contains() which can be expensive.
    boolean bool;//if true then all corners intersect, if false then no corners intersect
    // for partial, we exit early with 1 and ignore bool.
    bool = (contains(r.getMinX(),r.getMinY()));
    if (contains(r.getMinX(),r.getMaxY())) {
      if (!bool)
        return 1;//partial
    } else {
      if (bool)
        return 1;//partial
    }
    if (contains(r.getMaxX(),r.getMinY())) {
      if (!bool)
        return 1;//partial
    } else {
      if (bool)
        return 1;//partial
    }
    if (contains(r.getMaxX(),r.getMaxY())) {
      if (!bool)
        return 1;//partial
    } else {
      if (bool)
        return 1;//partial
    }
    return bool?4:0;
  }

  @Override
  public String toString() {
    //I'm deliberately making this look basic and not fully detailed with class name & misc fields.
    //Add distance in degrees, which is easier to recognize, and earth radius agnostic.
    String dStr = String.format("%.1f",distance);
    if (ctx.isGeo()) {
      double distDEG = ctx.getDistCalc().distanceToDegrees(distance);
      dStr += String.format("=%.1f\u00B0",distDEG);
    }
    return "Circle(" + point + ",d=" + dStr + ')';
  }
}
