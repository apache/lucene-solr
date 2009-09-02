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

import java.util.HashMap;
import java.util.Map;

/**
 * Provide a high level access point to distances
 * Used by DistanceSortSource and DistanceQuery
 *  
 *
 */
public class DistanceHandler {
  
  private Map<Integer,Double> distances;
  public enum Precision {EXACT, TWOFEET, TWENTYFEET, TWOHUNDREDFEET};
  private Precision precise;
  
  public DistanceHandler (Map<Integer,Double> distances, Precision precise){
    this.distances = distances;
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
      return DistanceUtils.getInstance().getDistanceMi(centerLat, centerLng, lat, lng);
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
      
      String k = new Double(xLat).toString() +","+ new Double(xLng).toString();
    
      Double d = (distances.get(k));
      if (d != null){
        return d.doubleValue();
      }
    }
    
    //all else fails calculate the distances    
    return DistanceUtils.getInstance().getDistanceMi(centerLat, centerLng, lat, lng);
  }
  
  
  public static void main(String args[]){ 
    DistanceHandler db = new DistanceHandler(new HashMap(), Precision.TWOHUNDREDFEET);
    System.out.println(DistanceHandler.getPrecision(-1234.123456789, db.getPrecision()));
  }
}
