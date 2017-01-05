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
package org.apache.lucene.spatial3d.geom;

/**
 * Factory for {@link GeoBBox}.
 *
 * @lucene.experimental
 */
public class GeoBBoxFactory {
  private GeoBBoxFactory() {
  }

  /**
   * Create a geobbox of the right kind given the specified bounds.
   *
   * @param planetModel is the planet model
   * @param topLat    is the top latitude
   * @param bottomLat is the bottom latitude
   * @param leftLon   is the left longitude
   * @param rightLon  is the right longitude
   * @return a GeoBBox corresponding to what was specified.
   */
  public static GeoBBox makeGeoBBox(final PlanetModel planetModel, double topLat, double bottomLat, double leftLon, double rightLon) {
    //System.err.println("Making rectangle for topLat="+topLat*180.0/Math.PI+", bottomLat="+bottomLat*180.0/Math.PI+", leftLon="+leftLon*180.0/Math.PI+", rightlon="+rightLon*180.0/Math.PI);
    if (topLat > Math.PI * 0.5)
      topLat = Math.PI * 0.5;
    if (bottomLat < -Math.PI * 0.5)
      bottomLat = -Math.PI * 0.5;
    if (leftLon < -Math.PI)
      leftLon = -Math.PI;
    if (rightLon > Math.PI)
      rightLon = Math.PI;
    if ((Math.abs(leftLon + Math.PI) < Vector.MINIMUM_ANGULAR_RESOLUTION && Math.abs(rightLon - Math.PI) < Vector.MINIMUM_ANGULAR_RESOLUTION) ||
        (Math.abs(rightLon + Math.PI) < Vector.MINIMUM_ANGULAR_RESOLUTION && Math.abs(leftLon - Math.PI) < Vector.MINIMUM_ANGULAR_RESOLUTION)) {
      if (Math.abs(topLat - Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION && Math.abs(bottomLat + Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION)
        return new GeoWorld(planetModel);
      if (Math.abs(topLat - bottomLat) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
        if (Math.abs(topLat - Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION || Math.abs(topLat + Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION)
          return new GeoDegeneratePoint(planetModel, topLat, 0.0);
        return new GeoDegenerateLatitudeZone(planetModel, topLat);
      }
      if (Math.abs(topLat - Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION)
        return new GeoNorthLatitudeZone(planetModel, bottomLat);
      else if (Math.abs(bottomLat + Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION)
        return new GeoSouthLatitudeZone(planetModel, topLat);
      return new GeoLatitudeZone(planetModel, topLat, bottomLat);
    }
    //System.err.println(" not latitude zone");
    double extent = rightLon - leftLon;
    if (extent < 0.0)
      extent += Math.PI * 2.0;
    if (topLat == Math.PI * 0.5 && bottomLat == -Math.PI * 0.5) {
      if (Math.abs(leftLon - rightLon) < Vector.MINIMUM_ANGULAR_RESOLUTION)
        return new GeoDegenerateLongitudeSlice(planetModel, leftLon);

      if (extent >= Math.PI)
        return new GeoWideLongitudeSlice(planetModel, leftLon, rightLon);

      return new GeoLongitudeSlice(planetModel, leftLon, rightLon);
    }
    //System.err.println(" not longitude slice");
    if (Math.abs(leftLon - rightLon) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
      if (Math.abs(topLat - bottomLat) < Vector.MINIMUM_ANGULAR_RESOLUTION)
        return new GeoDegeneratePoint(planetModel, topLat, leftLon);
      return new GeoDegenerateVerticalLine(planetModel, topLat, bottomLat, leftLon);
    }
    //System.err.println(" not vertical line");
    if (extent >= Math.PI) {
      if (Math.abs(topLat - bottomLat) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
        //System.err.println(" wide degenerate line");
        return new GeoWideDegenerateHorizontalLine(planetModel, topLat, leftLon, rightLon);
      }
      if (Math.abs(topLat - Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
        return new GeoWideNorthRectangle(planetModel, bottomLat, leftLon, rightLon);
      } else if (Math.abs(bottomLat + Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
        return new GeoWideSouthRectangle(planetModel, topLat, leftLon, rightLon);
      }
      //System.err.println(" wide rect");
      return new GeoWideRectangle(planetModel, topLat, bottomLat, leftLon, rightLon);
    }
    if (Math.abs(topLat - bottomLat) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
      if (Math.abs(topLat - Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION || Math.abs(topLat + Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
        return new GeoDegeneratePoint(planetModel, topLat, 0.0);
      }
      //System.err.println(" horizontal line");
      return new GeoDegenerateHorizontalLine(planetModel, topLat, leftLon, rightLon);
    }
    if (Math.abs(topLat - Math.PI * 0.5) < Vector.MINIMUM_ANGULAR_RESOLUTION) {
      return new GeoNorthRectangle(planetModel, bottomLat, leftLon, rightLon);
    } else if (Math.abs(bottomLat + Math.PI * 0.5) <  Vector.MINIMUM_ANGULAR_RESOLUTION) {
      return new GeoSouthRectangle(planetModel, topLat, leftLon, rightLon);
    }
    //System.err.println(" rectangle");
    return new GeoRectangle(planetModel, topLat, bottomLat, leftLon, rightLon);
  }

}
