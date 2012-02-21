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

package org.apache.lucene.spatial.base.prefix;

import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.context.simple.SimpleSpatialContext;
import org.apache.lucene.spatial.base.distance.DistanceUnits;
import org.apache.lucene.spatial.base.prefix.quad.QuadPrefixTree;
import org.apache.lucene.spatial.base.shape.Shape;
import org.apache.lucene.spatial.base.shape.simple.PointImpl;
import org.apache.lucene.spatial.base.shape.simple.RectangleImpl;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;


/**
 */
public class TestGridMatchInfo {

  @Test @Ignore
  public void testMatchInfo() {
    // Check Validation
    SpatialContext ctx = new SimpleSpatialContext(DistanceUnits.CARTESIAN,null,new RectangleImpl(0,10,0,10));
    QuadPrefixTree grid = new QuadPrefixTree(ctx, 2);


//    GeometricShapeFactory gsf = new GeometricShapeFactory();
//    gsf.setCentre( new com.vividsolutions.jts.geom.Coordinate( 5,5 ) );
//    gsf.setSize( 9.5 );
//    Shape shape = new JtsGeometry( gsf.createCircle() );

    Shape shape = new RectangleImpl(0, 6, 5, 10);

    shape = new PointImpl(3, 3);

    //TODO UPDATE BASED ON NEW API
    List<String> m = SpatialPrefixTree.nodesToTokenStrings(grid.getNodes(shape,3,false));
    System.out.println(m);

    for (CharSequence s : m) {
      System.out.println(s);
    }


//    // query should intersect everything one level down
//    ArrayList<String> descr = new ArrayList<String>();
//    descr.add( "AAA*" );
//    descr.add( "AABC*" );
//    System.out.println( descr );
  }
}
