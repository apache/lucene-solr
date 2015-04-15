package org.apache.lucene.spatial.spatial4j.geo3d;

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

public class GeoBBoxFactory
{
    private GeoBBoxFactory() {
    }
  
    /** Create a geobbox of the right kind given the specified bounds.
     *@param topLat is the top latitude
     *@param bottomLat is the bottom latitude
     *@param leftLon is the left longitude
     *@param rightLon is the right longitude
     *@return a GeoBBox corresponding to what was specified.
     */
    public static GeoBBox makeGeoBBox(double topLat, double bottomLat, double leftLon, double rightLon) {
        if (topLat > Math.PI * 0.5)
            topLat = Math.PI * 0.5;
        if (bottomLat < -Math.PI * 0.5)
            bottomLat = -Math.PI * 0.5;
        if (leftLon < -Math.PI)
            leftLon = -Math.PI;
        if (rightLon > Math.PI)
            rightLon = Math.PI;
        if (leftLon == -Math.PI && rightLon == Math.PI) {
            if (topLat == Math.PI * 0.5 && bottomLat == -Math.PI * 0.5)
                return new GeoWorld();
            if (topLat == bottomLat)
                return new GeoDegenerateLatitudeZone(topLat);
            return new GeoLatitudeZone(topLat, bottomLat);
        }
        double extent = rightLon - leftLon;
        if (extent < 0.0)
          extent += Math.PI * 2.0;
        if (topLat == Math.PI * 0.5 && bottomLat == -Math.PI * 0.5) {
          if (leftLon == rightLon)
            return new GeoDegenerateLongitudeSlice(leftLon);

          if (extent >= Math.PI)
            return new GeoWideLongitudeSlice(leftLon, rightLon);
          
          return new GeoLongitudeSlice(leftLon, rightLon);
        }
        if (leftLon == rightLon) {
          if (topLat == bottomLat)
            return new GeoDegeneratePoint(topLat, leftLon);
          return new GeoDegenerateVerticalLine(topLat, bottomLat, leftLon);
        }
        if (extent >= Math.PI) {
          if (topLat == bottomLat) {
            return new GeoWideDegenerateHorizontalLine(topLat, leftLon, rightLon);
          }
          return new GeoWideRectangle(topLat, bottomLat, leftLon, rightLon);
        }
        if (topLat == bottomLat)
          return new GeoDegenerateHorizontalLine(topLat, leftLon, rightLon);
        return new GeoRectangle(topLat, bottomLat, leftLon, rightLon);
    }

}
