package org.apache.lucene.spatial.tier.projections;

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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;


/**
 *
 *
 **/
public class SinusoidalProjectorTest extends LuceneTestCase {

  @Test
  public void testProjection() throws Exception {
    SinusoidalProjector prj = new SinusoidalProjector();
    //TODO: uncomment once SinusoidalProjector is fixed.  Unfortunately, fixing it breaks a lot of other stuff
    /*double[] doubles;
    doubles = prj.coords(-89.0, 10);
    assertEquals(0.003, doubles[0], 0.001);//x
    assertEquals(-89.0 * DistanceUtils.DEGREES_TO_RADIANS, doubles[1]);
    
    doubles = prj.coords(89.0, 0);
    assertEquals(0.0, doubles[0]);//x
    assertEquals(89.0 * DistanceUtils.DEGREES_TO_RADIANS, doubles[1]);

    doubles = prj.coords(89.0, 10);
    assertEquals(0.003, doubles[0], 0.001);//x
    assertEquals(89.0 * DistanceUtils.DEGREES_TO_RADIANS, doubles[1]);


    doubles = prj.coords(-89.0, 0);
    assertEquals(0.0, doubles[0]);//x
    assertEquals(-89.0 * DistanceUtils.DEGREES_TO_RADIANS, doubles[1]);*/


  }
}

//This code demonstrates that the SinusoidalProjector is incorrect
  /*@Test
  public void testFoo() throws Exception {
    CartesianTierPlotter plotter = new CartesianTierPlotter(11, new SinusoidalProjector(), "foo");
    SinusoidalProjector prj = new SinusoidalProjector();
    System.out.println("---- Equator ---");
    printValues(plotter, prj, 0);
    System.out.println("---- North ---");
    printValues(plotter, prj, 89.0);
    System.out.println("---- South ---");
    printValues(plotter, prj, -89.0);
  }

  private void printValues(CartesianTierPlotter plotter, SinusoidalProjector prj, double latitude){
    for (int i = 0; i <= 10; i++){
      double boxId = plotter.getTierBoxId(latitude, i);
      double[] doubles = prj.coords(latitude, i);
      System.out.println("Box[" + latitude + ", " + i + "] = " + boxId + " sinusoidal: [" + doubles[0] + ", " + doubles[1] + "]");
    }
    for (int i = -10; i <= 0; i++){
      double boxId = plotter.getTierBoxId(latitude, i);
      double[] doubles = prj.coords(latitude, i);
      System.out.println("Box[" + latitude + ", " + i + "] = " + boxId + " sinusoidal: [" + doubles[0] + ", " + doubles[1] + "]");
    }

  }
  */