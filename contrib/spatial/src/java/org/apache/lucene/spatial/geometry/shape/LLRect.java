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

package org.apache.lucene.spatial.geometry.shape;

import org.apache.lucene.spatial.geometry.FloatLatLng;
import org.apache.lucene.spatial.geometry.LatLng;



/**
 * Lat-long rect.  Instances are mutable.
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class LLRect {
  private LatLng ll, ur;
  
  public LLRect(LatLng ll, LatLng ur) {
    this.ll=ll;
    this.ur=ur;
  }
  
  public LLRect(LLRect other) {
    this.ll=other.ll;
    this.ur=other.ur;
  }
  
  /**
   * Return the area in units of lat-lng squared.  This is a contrived unit
   * that only has value when comparing to something else.
   */
  public double area() {
    return Math.abs((ll.getLat()-ur.getLat()) * (ll.getLng()-ur.getLng()));
  }

  public LatLng getLowerLeft() {
    return ll;
  }
  
  public LatLng getUpperRight() {
    return ur;
  }
  
  @Override
  public String toString() {
    return "{" + ll + ", " + ur + "}";
  }

  public LatLng getMidpoint() {
    return ll.calculateMidpoint(ur);
  }

  /**
   * Approximates a box centered at the given point with the given width and height in miles.
   * @param center
   * @param widthMi
   * @param heightMi
   */
  public static LLRect createBox(LatLng center, double widthMi, double heightMi) {
    double miplatdeg=DistanceApproximation.getMilesPerLngDeg(center.getLat());
    double miplngdeg=DistanceApproximation.getMilesPerLatDeg();
    
    double lngDelta=(widthMi/2)/miplngdeg;
    double latDelta=(heightMi/2)/miplatdeg;
    
    // TODO: Prob only works in northern hemisphere?
    LatLng ll=new FloatLatLng(center.getLat()-latDelta, center.getLng()-lngDelta);
    LatLng ur=new FloatLatLng(center.getLat()+latDelta, center.getLng()+lngDelta);
    
    return new LLRect(ll, ur);
  }
  
  /**
   * Returns a rectangle shape for the bounding box
   */
  public Rectangle toRectangle() {
    return new Rectangle(ll.getLng(), ll.getLat(), ur.getLng(), ur.getLat());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ll == null) ? 0 : ll.hashCode());
    result = prime * result + ((ur == null) ? 0 : ur.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    LLRect other = (LLRect) obj;
    if (ll == null) {
      if (other.ll != null)
        return false;
    } else if (!ll.equals(other.ll))
      return false;
    if (ur == null) {
      if (other.ur != null)
        return false;
    } else if (!ur.equals(other.ur))
      return false;
    return true;
  }
  
  
}
