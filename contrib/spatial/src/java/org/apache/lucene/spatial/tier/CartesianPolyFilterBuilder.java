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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.geometry.shape.Rectangle;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;


/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class CartesianPolyFilterBuilder {

  // Finer granularity than 1 mile isn't accurate with
  // standard java math.  Also, there's already a 2nd
  // precise filter, if needed, in DistanceQueryBuilder,
  // that will make the filtering exact.
  public static final double MILES_FLOOR = 1.0;

  private IProjector projector = new SinusoidalProjector();
  private Logger log = Logger.getLogger(getClass().getName());
  
  private final String tierPrefix;
  
  public CartesianPolyFilterBuilder( String tierPrefix ) {
    this.tierPrefix = tierPrefix;
  }
  
  public Shape getBoxShape(double latitude, double longitude, double miles)
  {  
    if (miles < MILES_FLOOR) {
      miles = MILES_FLOOR;
    }
    Rectangle box = DistanceUtils.getInstance().getBoundary(latitude, longitude, miles);
    double latY = box.getMaxPoint().getY();//box.getY();
    double latX = box.getMinPoint().getY() ; //box.getMaxY();
    
    double longY = box.getMaxPoint().getX(); ///box.getX();
    double longX = box.getMinPoint().getX();//box.getMaxX();
    
    CartesianTierPlotter ctp = new CartesianTierPlotter(2, projector,tierPrefix);
    int bestFit = ctp.bestFit(miles);
    
    log.info("Best Fit is : " + bestFit);
    ctp = new CartesianTierPlotter(bestFit, projector,tierPrefix);
    Shape shape = new Shape(ctp.getTierFieldName());
    
    // generate shape
    // iterate from startX->endX
    //     iterate from startY -> endY
    //      shape.add(currentLat.currentLong);
    
   
    double beginAt = ctp.getTierBoxId(latX, longX);
    double endAt = ctp.getTierBoxId(latY, longY);
    
    double tierVert = ctp.getTierVerticalPosDivider();
    log.fine(" | "+ beginAt+" | "+ endAt);
    
    double startX = beginAt - (beginAt %1);
    double startY = beginAt - startX ; //should give a whole number
    
    double endX = endAt - (endAt %1);
    double endY = endAt -endX; //should give a whole number
    
    int scale = (int)Math.log10(tierVert);
    endY = new BigDecimal(endY).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
    startY = new BigDecimal(startY).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
    if(log.isLoggable(Level.FINE)) {
      log.fine("scale "+scale+" startX "+ startX + " endX "+endX +" startY "+ startY + " endY "+ endY +" tierVert "+ tierVert);
    }
    double xInc = 1.0d / tierVert;
    xInc = new BigDecimal(xInc).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
    
    for (; startX <= endX; startX++){
      
      double itY = startY;
      while (itY <= endY){
        //create a boxId
        // startX.startY
        double boxId = startX + itY ;
        shape.addBox(boxId);
        //System.out.println("----"+boxId);
        itY += xInc;
        
        // java keeps 0.0001 as 1.0E-1
        // which ends up as 0.00011111
        itY = new BigDecimal(itY).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
      }
    }
    return shape;
  }
  
  public Filter getBoundingArea(double latitude, double longitude, double miles) 
  {
    Shape shape = getBoxShape(latitude, longitude, miles);
    return new CartesianShapeFilter(shape, shape.getTierId());
  }
}
