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

import org.apache.lucene.spatial.geometry.DistanceUnits;
import org.apache.lucene.spatial.geometry.FloatLatLng;
import org.apache.lucene.spatial.geometry.LatLng;
import org.apache.lucene.spatial.geometry.shape.LLRect;
import org.apache.lucene.spatial.geometry.shape.Rectangle;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class DistanceUtils {

  static DistanceUtils instance = new DistanceUtils();
  
  public static DistanceUtils getInstance()
  {
    return instance;
  }

  
  public double getDistanceMi(double x1, double y1, double x2, double y2) {
    return getLLMDistance(x1, y1, x2, y2);
  }

  /**
   * 
   * @param x1
   * @param y1
   * @param miles
   * @return boundary rectangle where getY/getX is top left, getMinY/getMinX is bottom right
   */
  public Rectangle getBoundary (double x1, double y1, double miles) {

    LLRect box = LLRect.createBox( new FloatLatLng( x1, y1 ), miles, miles );
    
    //System.out.println("Box: "+maxX+" | "+ maxY+" | "+ minX + " | "+ minY);
    return box.toRectangle();

  }
  
  public double getLLMDistance (double x1, double y1, double x2, double y2) {  

    LatLng p1 = new FloatLatLng( x1, y1 );
    LatLng p2 = new FloatLatLng( x2, y2 );
    return p1.arcDistance( p2, DistanceUnits.MILES );
  }
}
