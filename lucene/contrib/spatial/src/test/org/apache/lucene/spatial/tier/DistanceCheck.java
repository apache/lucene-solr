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

import org.apache.lucene.spatial.DistanceUtils;

import java.text.DecimalFormat;


public class DistanceCheck {

  /**
   * @param args
   */
  public static void main(String[] args) {
    double lat1 = 0;
    double long1 = 0;
    double lat2 = 0;
    double long2 = 0;
    
    for (int i =0; i < 90; i++){
      double dis = DistanceUtils.getDistanceMi(lat1, long1, lat2, long2);
      lat1 +=1;
      lat2 = lat1 + 0.001;
      
      System.out.println(lat1+","+long1+","+lat2+","+long2+","+formatDistance(dis));
      
    }

  }

  public static String formatDistance (Double d){
    DecimalFormat df1 = new DecimalFormat("####.000000");
    return df1.format(d);
  }
  
}
