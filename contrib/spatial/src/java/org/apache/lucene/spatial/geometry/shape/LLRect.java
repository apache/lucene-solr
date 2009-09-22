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
    double d = widthMi;
    LatLng ur = boxCorners(center, d, 45.0); // assume right angles
    LatLng ll = boxCorners(center, d, 225.0);

    //System.err.println("boxCorners: ur " + ur.getLat() + ',' + ur.getLng());
    //System.err.println("boxCorners: cnt " + center.getLat() + ',' + center.getLng());
    //System.err.println("boxCorners: ll " + ll.getLat() + ',' + ll.getLng());
    return new LLRect(ll, ur);
  }
  
  /**
   * Returns a rectangle shape for the bounding box
   */
  public Rectangle toRectangle() {
    return new Rectangle(ll.getLng(), ll.getLat(), ur.getLng(), ur.getLat());
  }

  private static LatLng boxCorners(LatLng center, double d, double brngdeg) {
    double a = center.getLat();
    double b = center.getLng();
    double R = 3963.0; // radius of earth in miles
    double brng = (Math.PI*brngdeg/180);
    double lat1 = (Math.PI*a/180);
    double lon1 = (Math.PI*b/180);

    // Haversine formula
    double lat2 = Math.asin( Math.sin(lat1)*Math.cos(d/R) +
                             Math.cos(lat1)*Math.sin(d/R)*Math.cos(brng) );
    double lon2 = lon1 + Math.atan2(Math.sin(brng)*Math.sin(d/R)*Math.cos(lat1),
                                    Math.cos(d/R)-Math.sin(lat1)*Math.sin(lat2));

    lat2 = (lat2*180)/Math.PI;
    lon2 = (lon2*180)/Math.PI;

    // normalize long first
    LatLng ll = normLng(lat2,lon2);

    // normalize lat - could flip poles
    ll = normLat(ll.getLat(),ll.getLng());

    return ll;
}

  /**
   * Returns a normalized Lat rectangle shape for the bounding box
   * If you go over the poles, you need to flip the lng value too
   */
  private static LatLng normLat(double lat, double lng) {
    if (lat > 90.0) {
        lat = 90.0 - (lat - 90.0);
        if (lng < 0) {
                lng = lng+180;
        } else {
                lng=lng-180;
        }
    }
    else if (lat < -90.0) {
        lat = -90.0 - (lat + 90.0);
        if (lng < 0) {
                lng = lng+180;
        } else {
                lng=lng-180;
        }
    }
    LatLng ll=new FloatLatLng(lat, lng);
    return ll;
  }

  /**
   * Returns a normalized Lng rectangle shape for the bounding box
   */
  private static LatLng normLng(double lat,double lng) {
    if (lng > 180.0) {
        lng = -1.0*(180.0 - (lng - 180.0));
    }
    else if (lng < -180.0) {
        lng = (lng + 180.0)+180.0;
    }
    LatLng ll=new FloatLatLng(lat, lng);
    return ll;
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
