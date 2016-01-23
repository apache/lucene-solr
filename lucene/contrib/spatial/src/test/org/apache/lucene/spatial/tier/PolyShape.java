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


/**
 *
 */
public class PolyShape {

  private static double lat = 38.969398; 
  private static double lng= -77.386398;
  private static int miles = 1000;
  /**
   * @param args
   */
  public static void main(String[] args) {
  
    CartesianPolyFilterBuilder cpf = new CartesianPolyFilterBuilder( "_localTier" );
    cpf.getBoxShape(lat, lng, miles);
    
  }

}
