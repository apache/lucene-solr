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

package org.apache.lucene.spatial.geometry;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class FixedLatLng extends LatLng {
  public static final double SCALE_FACTOR=1000000;
  public static final int SCALE_FACTOR_INT=1000000;
  
  private int lat, lng;
  private boolean normalized;
  
  public FixedLatLng(int lat, int lng) {
    setLat(lat);
    setLng(lng);
  }
  
  public FixedLatLng(LatLng ll) {
    this.lat=ll.getFixedLat();
    this.lng=ll.getFixedLng();
  }
  
  protected void setLat(int lat) {
    if (lat>90*SCALE_FACTOR || lat<-90*SCALE_FACTOR) {
      throw new IllegalArgumentException("Illegal lattitude");
    }
    this.lat=lat;
  }

  protected void setLng(int lng) {
    this.lng=lng;
  }
  
  public static double fixedToDouble(int fixed) {
    return (fixed)/SCALE_FACTOR;
  }
  
  public static int doubleToFixed(double d) {
    return (int)(d*SCALE_FACTOR);
  }
  
  @Override
  public LatLng copy() {
    return new FixedLatLng(this);
  }

  @Override
  public int getFixedLat() {
    return lat;
  }

  @Override
  public int getFixedLng() {
    return lng;
  }

  @Override
  public double getLat() {
    return fixedToDouble(lat);
  }

  @Override
  public double getLng() {
    return fixedToDouble(lng);
  }

  @Override
  public boolean isFixedPoint() {
    return true;
  }

  @Override
  public FixedLatLng toFixed() {
    return this;
  }

  @Override
  public FloatLatLng toFloat() {
    return new FloatLatLng(this);
  }

  @Override
  public boolean isNormalized() {
    return 
      normalized || (
          (lng>=-180*SCALE_FACTOR_INT) &&
          (lng<=180*SCALE_FACTOR_INT)
          );
  }

  @Override
  public LatLng normalize() {
    if (isNormalized()) return this;
    
    int delta=0;
    if (lng<0) delta=360*SCALE_FACTOR_INT;
    if (lng>=0) delta=-360*SCALE_FACTOR_INT;
    
    int newLng=lng;
    while (newLng<=-180*SCALE_FACTOR_INT || newLng>=180*SCALE_FACTOR_INT) {
      newLng+=delta;
    }
    
    FixedLatLng ret=new FixedLatLng(lat, newLng);
    ret.normalized=true;
    return ret;
  }
  
  @Override
  public LatLng calculateMidpoint(LatLng other) {
    return new FixedLatLng(
        (lat+other.getFixedLat())/2,
        (lng+other.getFixedLng())/2);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime + lat;
    result = prime * result + lng;
    result = prime * result + (normalized ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    FixedLatLng other = (FixedLatLng) obj;
    if (lat != other.lat)
      return false;
    if (lng != other.lng)
      return false;
    if (normalized != other.normalized)
      return false;
    return true;
  }
  
}
