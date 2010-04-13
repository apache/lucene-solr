/**
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

package org.apache.lucene.spatial.tier;

import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.spatial.geometry.LatLng;
import org.apache.lucene.spatial.geometry.FloatLatLng;
import org.apache.lucene.spatial.geometry.shape.LLRect;


/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class CartesianPolyFilterBuilder {

  // Finer granularity than 1 mile isn't accurate with
  // standard java math.  Also, there's already a 2nd
  // precise filter, if needed, in DistanceQueryBuilder,
  // that will make the filtering exact.
  public static final double MILES_FLOOR = 1.0;

  private IProjector projector = new SinusoidalProjector();
  private final String tierPrefix;
	private int minTier;
	private int maxTier;
  /**
   * 
   * @param tierPrefix The prefix for the name of the fields containing the tier info
   * @param minTierIndexed The minimum tier level indexed
   * @param maxTierIndexed The maximum tier level indexed
   */
  public CartesianPolyFilterBuilder( String tierPrefix, int minTierIndexed, int maxTierIndexed ) {
    this.tierPrefix = tierPrefix;
	this.minTier = minTierIndexed;
	this.maxTier = maxTierIndexed;
  }
  
  public Shape getBoxShape(double latitude, double longitude, double miles)
  {  
    if (miles < MILES_FLOOR) {
      miles = MILES_FLOOR;
    }
    LLRect box1 = LLRect.createBox( new FloatLatLng( latitude, longitude ), miles, miles );
    LatLng lowerLeft = box1.getLowerLeft();
    LatLng upperRight = box1.getUpperRight();

    double latUpperRight = upperRight.getLat();
    double latLowerLeft = lowerLeft.getLat();
    double longUpperRight = upperRight.getLng();
    double longLowerLeft = lowerLeft.getLng();

    CartesianTierPlotter ctp = new CartesianTierPlotter( miles, projector, tierPrefix );
    Shape shape = new Shape(ctp.getTierLevelId());

    if (longUpperRight < longLowerLeft) { // Box cross the 180 meridian
      addBoxes(shape, ctp, latLowerLeft, longLowerLeft, latUpperRight, LatLng.LONGITUDE_DEGREE_MAX);
      addBoxes(shape, ctp, latLowerLeft, -LatLng.LONGITUDE_DEGREE_MIN, latUpperRight, longUpperRight);
    } else {
      addBoxes(shape, ctp, latLowerLeft, longLowerLeft, latUpperRight, longUpperRight);
    }
 
    return shape; 
  } 
  
  private void addBoxes(Shape shape, CartesianTierPlotter tierPlotter, double lat1, double long1, double lat2, double long2) {
    double boxId1 = tierPlotter.getTierBoxId(lat1, long1);
    double boxId2 = tierPlotter.getTierBoxId(lat2, long2);

    double tierVert = tierPlotter.getTierVerticalPosDivider();

    int LongIndex1 = (int) Math.round(boxId1);
    int LatIndex1 = (int) Math.round((boxId1 - LongIndex1) * tierVert);

    int LongIndex2 = (int) Math.round(boxId2);
    int LatIndex2 = (int) Math.round((boxId2 - LongIndex2) * tierVert);

    int startLong, endLong;
    int startLat, endLat;

    if (LongIndex1 > LongIndex2) {
      startLong = LongIndex2;
      endLong = LongIndex1;
    } else {
      startLong = LongIndex1;
      endLong = LongIndex2;
    }

    if (LatIndex1 > LatIndex2) {
      startLat = LatIndex2;
      endLat = LatIndex1;
    } else {
      startLat = LatIndex1;
      endLat = LatIndex2;
    }

    int LatIndex, LongIndex;
    for (LongIndex = startLong; LongIndex <= endLong; LongIndex++) {
      for (LatIndex = startLat; LatIndex <= endLat; LatIndex++) {
        // create a boxId
        double boxId = LongIndex + LatIndex / tierVert;
        shape.addBox(boxId);
      }
    }
  }
  
  public Filter getBoundingArea(double latitude, double longitude, double miles) 
  {
    Shape shape = getBoxShape(latitude, longitude, miles);
    return new CartesianShapeFilter(shape, tierPrefix + shape.getTierId());
  }
}
