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

/** This class represents a point on the surface of a unit sphere.
*/
public class GeoPoint extends Vector
{
    public GeoPoint(final double sinLat, final double sinLon, final double cosLat, final double cosLon)
    {
        super(cosLat*cosLon,cosLat*sinLon,sinLat);
    }
      
    public GeoPoint(final double lat, final double lon)
    {
        this(Math.sin(lat),Math.sin(lon),Math.cos(lat),Math.cos(lon));
    }
          
    public GeoPoint(final double x, final double y, final double z)
    {
        super(x,y,z);
    }
          
    public double arcDistance(final GeoPoint v)
    {
        return Tools.safeAcos(evaluate(v));
    }

    /** Find a single point that is a specified arc distance away from this point.
    */
    public GeoPoint getSamplePoint(final double sinRotationAngle, final double cosRotationAngle) {
        // Rotate in the best of three possible directions: x-y, x-z, y-z.
        final double absX = Math.abs(x);
        final double absY = Math.abs(y);
        final double absZ = Math.abs(z);
        if (absX > absY) {
            // x > y
            if (absY > absZ) {
                // x > y > z
                // rotate in x-y
                return new GeoPoint(x*cosRotationAngle-y*sinRotationAngle,x*sinRotationAngle+y*cosRotationAngle,z);
            } else {
                // x > z > y OR z > x > y
                // rotate in x-z
                return new GeoPoint(x*cosRotationAngle-z*sinRotationAngle,y,x*sinRotationAngle+z*cosRotationAngle);
            }
        } else {
            // y > x
            if (absX > absZ) {
                // y > x > z
                // rotate in x-y
                return new GeoPoint(x*cosRotationAngle-y*sinRotationAngle,x*sinRotationAngle+y*cosRotationAngle,z);
            } else {
                // y > z > x OR z > y > x
                // rotate in y-z
                return new GeoPoint(x,y*cosRotationAngle-z*sinRotationAngle,y*sinRotationAngle+z*cosRotationAngle);
            }
        }
    }
    
    /** Find a single point that is a specified arc distance away from this point.
    */
    public GeoPoint getSamplePoint(final double rotationAngle) {
        return getSamplePoint(Math.sin(rotationAngle), Math.cos(rotationAngle));
    }

}
