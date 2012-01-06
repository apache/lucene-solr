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

import org.apache.lucene.spatial.DistanceUtils;

import java.util.Map;

/**
 * Provide a high level access point to distances
 * Used by DistanceSortSource and DistanceQuery
 *  
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 *
 */
public class DistanceHandler {

  public enum Precision {EXACT, TWOFEET, TWENTYFEET, TWOHUNDREDFEET}
  
  private Map<Integer,Double> distances;
  private Map<String, Double> distanceLookupCache;
  private Precision precise;
  
  public DistanceHandler (Map<Integer,Double> distances, Map<String, Double> distanceLookupCache, Precision precise){
    this.distances = distances;
    this.distanceLookupCache = distanceLookupCache;
    this.precise = precise; 
  }
  
  
  public static double getPrecision(double x, Precision thisPrecise){
    
    if(thisPrecise != null){
      double dif = 0;
      switch(thisPrecise) {
        case EXACT: return x;
        case TWOFEET:        dif = x % 0.0001; break;
        case TWENTYFEET:     dif = x % 0.001;  break;
        case TWOHUNDREDFEET: dif = x % 0.01; break;
      }
      return x - dif;
    }
    return x;
  }
  
  public Precision getPrecision() {
    return precise;
  }
  
  public double getDistance(int docid, double centerLat, double centerLng, double lat, double lng){
  
    // check to see if we have distances
    // if not calculate the distance
    if(distances == null){
      return DistanceUtils.getDistanceMi(centerLat, centerLng, lat, lng);
    }
    
    // check to see if the doc id has a cached distance
    Double docd = distances.get( docid );
    if (docd != null){
      return docd.doubleValue();
    }
    
    //check to see if we have a precision code
    // and if another lat/long has been calculated at
    // that rounded location
    if (precise != null) {
      double xLat = getPrecision(lat, precise);
      double xLng = getPrecision(lng, precise);
      
      String k = Double.valueOf(xLat).toString() +","+ Double.valueOf(xLng).toString();
    
      Double d = (distanceLookupCache.get(k));
      if (d != null){
        return d.doubleValue();
      }
    }
    
    //all else fails calculate the distances    
    return DistanceUtils.getDistanceMi(centerLat, centerLng, lat, lng);
  }
}
