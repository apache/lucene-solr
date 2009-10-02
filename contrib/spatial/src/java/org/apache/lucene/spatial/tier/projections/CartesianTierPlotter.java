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

package org.apache.lucene.spatial.tier.projections;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class CartesianTierPlotter {
  public static final String DEFALT_FIELD_PREFIX = "_tier_";
  
  final int tierLevel;
  int tierLength;
  int tierBoxes;
  int tierVerticalPosDivider;
  final IProjector projector;
  final String fieldPrefix;
  Double idd = Double.valueOf(180);
  
  public CartesianTierPlotter (int tierLevel, IProjector projector, String fieldPrefix) {
  
    this.tierLevel  = tierLevel;
    this.projector = projector;
    this.fieldPrefix = fieldPrefix;
    
    setTierLength();
    setTierBoxes();
    setTierVerticalPosDivider();
  }
  
  private void setTierLength (){
    this.tierLength = (int) Math.pow(2 , this.tierLevel);
  }
  
  private void setTierBoxes () {
    this.tierBoxes = (int)Math.pow(this.tierLength, 2);
  }
  
  /**
   * Get nearest max power of 10 greater than
   * the tierlen
   * e.g
   * tierId of 13 has tierLen 8192
   * nearest max power of 10 greater than tierLen 
   * would be 10,000
   */
  
  private void setTierVerticalPosDivider() {
    
    // ceiling of log base 10 of tierLen
    
    tierVerticalPosDivider = Double.valueOf(Math.ceil(
          Math.log10(Integer.valueOf(this.tierLength).doubleValue()))).intValue();
    
    // 
    tierVerticalPosDivider = (int)Math.pow(10, tierVerticalPosDivider );
    
  }
  
  public double getTierVerticalPosDivider(){
    return tierVerticalPosDivider;
  }
  
  /**
   * TierBoxId is latitude box id + longitude box id
   * where latitude box id, and longitude box id are transposed in to position
   * coordinates.
   * 
   * @param latitude
   * @param longitude
   */
  public double getTierBoxId (double latitude, double longitude) {
    
    double[] coords = projector.coords(latitude, longitude);
    
    double id = getBoxId(coords[0]) + (getBoxId(coords[1]) / tierVerticalPosDivider);
    return id ;
  }
  
  
  private double getBoxId (double coord){
    
    
    return Math.floor(coord / (idd / this.tierLength));
  }
  
  @SuppressWarnings("unused")
  private double getBoxId (double coord, int tierLen){
    return Math.floor(coord / (idd / tierLen) );
  }
  /**
   * get the string name representing current tier
   * _localTier&lt;tiedId&gt;
   */
  public String getTierFieldName (){
    
    return fieldPrefix + this.tierLevel;
  }
  
  /**
   * get the string name representing tierId
   * _localTier&lt;tierId&gt;
   * @param tierId
   */
  public String getTierFieldName (int tierId){
    
    return fieldPrefix + tierId;
  }
  
  /**
   * Find the tier with the best fit for a bounding box
   * Best fit is defined as the ceiling of
   *  log2 (circumference of earth / distance) 
   *  distance is defined as the smallest box fitting
   *  the corner between a radius and a bounding box.
   *  
   *  Distances less than a mile return 15, finer granularity is
   *  in accurate
   */
  public int bestFit(double miles){
    
    //28,892 a rough circumference of the earth
    int circ = 28892;
    
    double r = miles / 2.0;
    
    double corner = r - Math.sqrt(Math.pow(r, 2) / 2.0d);
    double times = circ / corner;
    int bestFit =  (int)Math.ceil(log2(times)) + 1;
    
    if (bestFit > 15) {
      // 15 is the granularity of about 1 mile
      // finer granularity isn't accurate with standard java math
      return 15;
    }
    return bestFit;
  }
  
  /**
   * a log to the base 2 formula
   * <code>Math.log(value) / Math.log(2)</code>
   * @param value
   */
  public double log2(double value) {
    
    return Math.log(value) / Math.log(2);
  }
}
