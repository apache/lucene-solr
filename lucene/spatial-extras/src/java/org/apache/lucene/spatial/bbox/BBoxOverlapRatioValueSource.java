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
package org.apache.lucene.spatial.bbox;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;

import com.spatial4j.core.shape.Rectangle;

/**
 * The algorithm is implemented as envelope on envelope (rect on rect) overlays rather than
 * complex polygon on complex polygon overlays.
 * <p>
 * Spatial relevance scoring algorithm:
 * <DL>
 *   <DT>queryArea</DT> <DD>the area of the input query envelope</DD>
 *   <DT>targetArea</DT> <DD>the area of the target envelope (per Lucene document)</DD>
 *   <DT>intersectionArea</DT> <DD>the area of the intersection between the query and target envelopes</DD>
 *   <DT>queryTargetProportion</DT> <DD>A 0-1 factor that divides the score proportion between query and target.
 *   0.5 is evenly.</DD>
 *
 *   <DT>queryRatio</DT> <DD>intersectionArea / queryArea; (see note)</DD>
 *   <DT>targetRatio</DT> <DD>intersectionArea / targetArea; (see note)</DD>
 *   <DT>queryFactor</DT> <DD>queryRatio * queryTargetProportion;</DD>
 *   <DT>targetFactor</DT> <DD>targetRatio * (1 - queryTargetProportion);</DD>
 *   <DT>score</DT> <DD>queryFactor + targetFactor;</DD>
 * </DL>
 * Additionally, note that an optional minimum side length {@code minSideLength} may be used whenever an
 * area is calculated (queryArea, targetArea, intersectionArea). This allows for points or horizontal/vertical lines
 * to be used as the query shape and in such case the descending order should have smallest boxes up front. Without
 * this, a point or line query shape typically scores everything with the same value since there is 0 area.
 * <p>
 * Note: The actual computation of queryRatio and targetRatio is more complicated so that it considers
 * points and lines. Lines have the ratio of overlap, and points are either 1.0 or 0.0 depending on whether
 * it intersects or not.
 * <p>
 * Originally based on Geoportal's
 * <a href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialRankingValueSource.java">
 *   SpatialRankingValueSource</a> but modified quite a bit. GeoPortal's algorithm will yield a score of 0
 * if either a line or point is compared, and it doesn't output a 0-1 normalized score (it multiplies the factors),
 * and it doesn't support minSideLength, and it had dateline bugs.
 *
 * @lucene.experimental
 */
public class BBoxOverlapRatioValueSource extends BBoxSimilarityValueSource {

  private final boolean isGeo;//-180/+180 degrees  (not part of identity; attached to parent strategy/field)

  private final Rectangle queryExtent;
  private final double queryArea;//not part of identity

  private final double minSideLength;

  private final double queryTargetProportion;

  //TODO option to compute geodetic area

  /**
   *
   * @param rectValueSource mandatory; source of rectangles
   * @param isGeo True if ctx.isGeo() and thus dateline issues should be attended to
   * @param queryExtent mandatory; the query rectangle
   * @param queryTargetProportion see class javadocs. Between 0 and 1.
   * @param minSideLength see class javadocs. 0.0 will effectively disable.
   */
  public BBoxOverlapRatioValueSource(ValueSource rectValueSource, boolean isGeo, Rectangle queryExtent,
                                     double queryTargetProportion, double minSideLength) {
    super(rectValueSource);
    this.isGeo = isGeo;
    this.minSideLength = minSideLength;
    this.queryExtent = queryExtent;
    this.queryArea = calcArea(queryExtent.getWidth(), queryExtent.getHeight());
    assert queryArea >= 0;
    this.queryTargetProportion = queryTargetProportion;
    if (queryTargetProportion < 0 || queryTargetProportion > 1.0)
      throw new IllegalArgumentException("queryTargetProportion must be >= 0 and <= 1");
  }

  /** Construct with 75% weighting towards target (roughly GeoPortal's default), geo degrees assumed, no
   * minimum side length. */
  public BBoxOverlapRatioValueSource(ValueSource rectValueSource, Rectangle queryExtent) {
    this(rectValueSource, true, queryExtent, 0.25, 0.0);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;

    BBoxOverlapRatioValueSource that = (BBoxOverlapRatioValueSource) o;

    if (Double.compare(that.minSideLength, minSideLength) != 0) return false;
    if (Double.compare(that.queryTargetProportion, queryTargetProportion) != 0) return false;
    if (!queryExtent.equals(that.queryExtent)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    result = 31 * result + queryExtent.hashCode();
    temp = Double.doubleToLongBits(minSideLength);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(queryTargetProportion);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  protected String similarityDescription() {
    return queryExtent.toString() + "," + queryTargetProportion;
  }

  @Override
  protected double score(Rectangle target, AtomicReference<Explanation> exp) {
    // calculate "height": the intersection height between two boxes.
    double top = Math.min(queryExtent.getMaxY(), target.getMaxY());
    double bottom = Math.max(queryExtent.getMinY(), target.getMinY());
    double height = top - bottom;
    if (height < 0) {
      if (exp != null) {
        exp.set(Explanation.noMatch("No intersection"));
      }
      return 0;//no intersection
    }

    // calculate "width": the intersection width between two boxes.
    double width = 0;
    {
      Rectangle a = queryExtent;
      Rectangle b = target;
      if (a.getCrossesDateLine() == b.getCrossesDateLine()) {
        //both either cross or don't
        double left = Math.max(a.getMinX(), b.getMinX());
        double right = Math.min(a.getMaxX(), b.getMaxX());
        if (!a.getCrossesDateLine()) {//both don't
          if (left <= right) {
            width = right - left;
          } else if (isGeo && (Math.abs(a.getMinX()) == 180 || Math.abs(a.getMaxX()) == 180)
              && (Math.abs(b.getMinX()) == 180 || Math.abs(b.getMaxX()) == 180)) {
            width = 0;//both adjacent to dateline
          } else {
            if (exp != null) {
              exp.set(Explanation.noMatch("No intersection"));
            }
            return 0;//no intersection
          }
        } else {//both cross
          width = right - left + 360;
        }
      } else {
        if (!a.getCrossesDateLine()) {//then flip
          a = target;
          b = queryExtent;
        }
        //a crosses, b doesn't
        double qryWestLeft = Math.max(a.getMinX(), b.getMinX());
        double qryWestRight = b.getMaxX();
        if (qryWestLeft < qryWestRight)
          width += qryWestRight - qryWestLeft;

        double qryEastLeft = b.getMinX();
        double qryEastRight = Math.min(a.getMaxX(), b.getMaxX());
        if (qryEastLeft < qryEastRight)
          width += qryEastRight - qryEastLeft;

        if (qryWestLeft > qryWestRight && qryEastLeft > qryEastRight) {
          if (exp != null) {
            exp.set(Explanation.noMatch("No intersection"));
          }
          return 0;//no intersection
        }
      }
    }

    // calculate queryRatio and targetRatio
    double intersectionArea = calcArea(width, height);
    double queryRatio;
    if (queryArea > 0) {
      queryRatio = intersectionArea / queryArea;
    } else if (queryExtent.getHeight() > 0) {//vert line
      queryRatio = height / queryExtent.getHeight();
    } else if (queryExtent.getWidth() > 0) {//horiz line
      queryRatio = width / queryExtent.getWidth();
    } else {
      queryRatio = queryExtent.relate(target).intersects() ? 1 : 0;//could be optimized
    }

    double targetArea = calcArea(target.getWidth(), target.getHeight());
    assert targetArea >= 0;
    double targetRatio;
    if (targetArea > 0) {
      targetRatio = intersectionArea / targetArea;
    } else if (target.getHeight() > 0) {//vert line
      targetRatio = height / target.getHeight();
    } else if (target.getWidth() > 0) {//horiz line
      targetRatio = width / target.getWidth();
    } else {
      targetRatio = target.relate(queryExtent).intersects() ? 1 : 0;//could be optimized
    }
    assert queryRatio >= 0 && queryRatio <= 1 : queryRatio;
    assert targetRatio >= 0 && targetRatio <= 1 : targetRatio;

    // combine ratios into a score

    double queryFactor = queryRatio * queryTargetProportion;
    double targetFactor = targetRatio * (1.0 - queryTargetProportion);
    double score = queryFactor + targetFactor;

    if (exp!=null) {
      String minSideDesc = minSideLength > 0.0 ? " (minSide="+minSideLength+")" : "";
      exp.set(Explanation.match((float) score,
          this.getClass().getSimpleName()+": queryFactor + targetFactor",
          Explanation.match((float)intersectionArea, "IntersectionArea" + minSideDesc,
              Explanation.match((float)width, "width"),
              Explanation.match((float)height, "height"),
              Explanation.match((float)queryTargetProportion, "queryTargetProportion")),
          Explanation.match((float)queryFactor, "queryFactor",
              Explanation.match((float)targetRatio, "ratio"),
              Explanation.match((float)queryArea,  "area of " + queryExtent + minSideDesc)),
          Explanation.match((float)targetFactor, "targetFactor",
              Explanation.match((float)targetRatio, "ratio"),
              Explanation.match((float)targetArea,  "area of " + target + minSideDesc))));
    }

    return score;
  }

  /** Calculates the area while applying the minimum side length. */
  private double calcArea(double width, double height) {
    return Math.max(minSideLength, width) * Math.max(minSideLength, height);
  }

}
