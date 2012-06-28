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

import org.apache.lucene.search.Explanation;

import com.spatial4j.core.shape.Rectangle;

/**
 * The algorithm is implemented as envelope on envelope overlays rather than
 * complex polygon on complex polygon overlays.
 * <p/>
 * <p/>
 * Spatial relevance scoring algorithm:
 * <p/>
 * <br/>  queryArea = the area of the input query envelope
 * <br/>  targetArea = the area of the target envelope (per Lucene document)
 * <br/>  intersectionArea = the area of the intersection for the query/target envelopes
 * <br/>  queryPower = the weighting power associated with the query envelope (default = 1.0)
 * <br/>  targetPower =  the weighting power associated with the target envelope (default = 1.0)
 * <p/>
 * <br/>  queryRatio  = intersectionArea / queryArea;
 * <br/>  targetRatio = intersectionArea / targetArea;
 * <br/>  queryFactor  = Math.pow(queryRatio,queryPower);
 * <br/>  targetFactor = Math.pow(targetRatio,targetPower);
 * <br/>  score = queryFactor * targetFactor;
 * <p/>
 * Based on Geoportal's
 * <a href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialRankingValueSource.java">
 *   SpatialRankingValueSource</a>.
 *
 * @lucene.experimental
 */
public class AreaSimilarity implements BBoxSimilarity {
  /**
   * Properties associated with the query envelope
   */
  private final Rectangle queryExtent;
  private final double queryArea;

  private final double targetPower;
  private final double queryPower;

  public AreaSimilarity(Rectangle queryExtent, double queryPower, double targetPower) {
    this.queryExtent = queryExtent;
    this.queryArea = queryExtent.getArea();

    this.queryPower = queryPower;
    this.targetPower = targetPower;

//  if (this.qryMinX > queryExtent.getMaxX()) {
//    this.qryCrossedDateline = true;
//    this.qryArea = Math.abs(qryMaxX + 360.0 - qryMinX) * Math.abs(qryMaxY - qryMinY);
//  } else {
//    this.qryArea = Math.abs(qryMaxX - qryMinX) * Math.abs(qryMaxY - qryMinY);
//  }
  }

  public AreaSimilarity(Rectangle queryExtent) {
    this(queryExtent, 2.0, 0.5);
  }


  public String getDelimiterQueryParameters() {
    return queryExtent.toString() + ";" + queryPower + ";" + targetPower;
  }

  @Override
  public double score(Rectangle target, Explanation exp) {
    if (target == null || queryArea <= 0) {
      return 0;
    }
    double targetArea = target.getArea();
    if (targetArea <= 0) {
      return 0;
    }
    double score = 0;

    double top = Math.min(queryExtent.getMaxY(), target.getMaxY());
    double bottom = Math.max(queryExtent.getMinY(), target.getMinY());
    double height = top - bottom;
    double width = 0;

    // queries that cross the date line
    if (queryExtent.getCrossesDateLine()) {
      // documents that cross the date line
      if (target.getCrossesDateLine()) {
        double left = Math.max(queryExtent.getMinX(), target.getMinX());
        double right = Math.min(queryExtent.getMaxX(), target.getMaxX());
        width = right + 360.0 - left;
      } else {
        double qryWestLeft = Math.max(queryExtent.getMinX(), target.getMaxX());
        double qryWestRight = Math.min(target.getMaxX(), 180.0);
        double qryWestWidth = qryWestRight - qryWestLeft;
        if (qryWestWidth > 0) {
          width = qryWestWidth;
        } else {
          double qryEastLeft = Math.max(target.getMaxX(), -180.0);
          double qryEastRight = Math.min(queryExtent.getMaxX(), target.getMaxX());
          double qryEastWidth = qryEastRight - qryEastLeft;
          if (qryEastWidth > 0) {
            width = qryEastWidth;
          }
        }
      }
    } else { // queries that do not cross the date line

      if (target.getCrossesDateLine()) {
        double tgtWestLeft = Math.max(queryExtent.getMinX(), target.getMinX());
        double tgtWestRight = Math.min(queryExtent.getMaxX(), 180.0);
        double tgtWestWidth = tgtWestRight - tgtWestLeft;
        if (tgtWestWidth > 0) {
          width = tgtWestWidth;
        } else {
          double tgtEastLeft = Math.max(queryExtent.getMinX(), -180.0);
          double tgtEastRight = Math.min(queryExtent.getMaxX(), target.getMaxX());
          double tgtEastWidth = tgtEastRight - tgtEastLeft;
          if (tgtEastWidth > 0) {
            width = tgtEastWidth;
          }
        }
      } else {
        double left = Math.max(queryExtent.getMinX(), target.getMinX());
        double right = Math.min(queryExtent.getMaxX(), target.getMaxX());
        width = right - left;
      }
    }


    // calculate the score
    if ((width > 0) && (height > 0)) {
      double intersectionArea = width * height;
      double queryRatio = intersectionArea / queryArea;
      double targetRatio = intersectionArea / targetArea;
      double queryFactor = Math.pow(queryRatio, queryPower);
      double targetFactor = Math.pow(targetRatio, targetPower);
      score = queryFactor * targetFactor * 10000.0;

      if (exp!=null) {
//        StringBuilder sb = new StringBuilder();
//        sb.append("\nscore=").append(score);
//        sb.append("\n  query=").append();
//        sb.append("\n  target=").append(target.toString());
//        sb.append("\n  intersectionArea=").append(intersectionArea);
//        
//        sb.append(" queryArea=").append(queryArea).append(" targetArea=").append(targetArea);
//        sb.append("\n  queryRatio=").append(queryRatio).append(" targetRatio=").append(targetRatio);
//        sb.append("\n  queryFactor=").append(queryFactor).append(" targetFactor=").append(targetFactor);
//        sb.append(" (queryPower=").append(queryPower).append(" targetPower=").append(targetPower).append(")");
        
        exp.setValue((float)score);
        exp.setDescription(this.getClass().getSimpleName());
        
        Explanation e = null;
        
        exp.addDetail( e = new Explanation((float)intersectionArea, "IntersectionArea") );
        e.addDetail(new Explanation((float)width,  "width; Query: "+queryExtent.toString()));
        e.addDetail(new Explanation((float)height, "height; Target: "+target.toString()));

        exp.addDetail( e = new Explanation((float)queryFactor, "Query") );
        e.addDetail(new Explanation((float)queryArea, "area"));
        e.addDetail(new Explanation((float)queryRatio, "ratio"));
        e.addDetail(new Explanation((float)queryPower, "power"));

        exp.addDetail( e = new Explanation((float)targetFactor, "Target") );
        e.addDetail(new Explanation((float)targetArea, "area"));
        e.addDetail(new Explanation((float)targetRatio, "ratio"));
        e.addDetail(new Explanation((float)targetPower, "power"));
      }
    }
    else if(exp !=null) {
      exp.setValue(0);
      exp.setDescription("Shape does not intersect");
    }
    return score;
  }


  /**
   * Determines if this ValueSource is equal to another.
   *
   * @param o the ValueSource to compare
   * @return <code>true</code> if the two objects are based upon the same query envelope
   */
  @Override
  public boolean equals(Object o) {
    if (o.getClass() != AreaSimilarity.class)
      return false;

    AreaSimilarity other = (AreaSimilarity) o;
    return getDelimiterQueryParameters().equals(other.getDelimiterQueryParameters());
  }

  /**
   * Returns the ValueSource hash code.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return getDelimiterQueryParameters().hashCode();
  }
}
