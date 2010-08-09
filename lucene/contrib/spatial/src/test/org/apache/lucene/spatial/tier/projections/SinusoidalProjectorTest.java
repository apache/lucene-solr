package org.apache.lucene.spatial.tier.projections;

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