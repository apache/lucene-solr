package org.apache.lucene.bkdtree3d;

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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.bkdtree3d.BKD3DTreeDocValuesFormat.encodeValue;

public class TestBKD3DTree extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);

    BKD3DTreeWriter w = new BKD3DTreeWriter();

    w.add(0, 0, 0, 0);
    w.add(1, 1, 1, 1);
    w.add(-1, -1, -1, 2);

    long indexFP = w.finish(out);
    out.close();

    IndexInput in = dir.openInput("bkd", IOContext.DEFAULT);
    in.seek(indexFP);
    BKD3DTreeReader r = new BKD3DTreeReader(in, 3);

    DocIdSet hits = r.intersect(Integer.MIN_VALUE, Integer.MAX_VALUE,
                                Integer.MIN_VALUE, Integer.MAX_VALUE,
                                Integer.MIN_VALUE, Integer.MAX_VALUE,

                                new BKD3DTreeReader.ValueFilter() {

                                  @Override
                                  public boolean accept(int docID) {
                                    return true;
                                  }

                                  @Override
                                  public BKD3DTreeReader.Relation compare(int xMin, int xMax,
                                                          int yMin, int yMax,
                                                          int zMin, int zMax) {
                                    return BKD3DTreeReader.Relation.INSIDE;
                                  }

                                });
    DocIdSetIterator disi = hits.iterator();
    assertEquals(0, disi.nextDoc());
    assertEquals(1, disi.nextDoc());
    assertEquals(2, disi.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, disi.nextDoc());
    in.close();
    dir.close();
  }

  static class Point {
    final double x;
    final double y;
    final double z;

    public Point(double x, double y, double z) {
      this.x = x;
      this.y = y;
      this.z = z;
    }

    @Override
    public String toString() {
      return "x=" + x + " y=" + y + " z=" + z;
    }
  }

  private static class Range {
    final double min;
    final double max;

    public Range(double min, double max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public String toString() {
      return min + " TO " + max;
    }
  }

  private double randomCoord() {
    // nocommit
    //return (random().nextDouble()*2.0022) - 1.0011;
    return (random().nextDouble()*.002) - .001;
  }

  private Range randomRange() {
    double x = randomCoord();
    double y = randomCoord();
    if (x < y) {
      return new Range(x, y);
    } else {
      return new Range(y, x);
    }
  }

  public void testRandom() throws Exception {
    List<Point> points = new ArrayList<>();
    // nocommit
    //int numPoints = atLeast(10000);
    int numPoints = atLeast(200);
    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048); 

    // nocommit
    maxPointsInLeaf = 100;

    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);

    BKD3DTreeWriter w = new BKD3DTreeWriter(maxPointsInLeaf, maxPointsSortInHeap);
    for(int docID=0;docID<numPoints;docID++) {
      Point point;
      if (docID > 0 && random().nextInt(30) == 17) {
        // Dup point
        point = points.get(random().nextInt(points.size()));
      } else {
        point = new Point(randomCoord(),
                          randomCoord(),
                          randomCoord());
      }

      if (VERBOSE) {
        System.out.println("  docID=" + docID + " point=" + point);
        System.out.println("    x=" + encodeValue(point.x) +
                           " y=" + encodeValue(point.y) +
                           " z=" + encodeValue(point.z));
      }

      points.add(point);
      w.add(encodeValue(point.x),
            encodeValue(point.y),
            encodeValue(point.z),
            docID);
    }

    long indexFP = w.finish(out);
    out.close();

    IndexInput in = dir.openInput("bkd", IOContext.DEFAULT);
    in.seek(indexFP);
    BKD3DTreeReader r = new BKD3DTreeReader(in, numPoints);

    int numIters = atLeast(100);
    for(int iter=0;iter<numIters;iter++) {
      // bbox
      Range x = randomRange();
      Range y = randomRange();
      Range z = randomRange();

      int xMinEnc = encodeValue(x.min);
      int xMaxEnc = encodeValue(x.max);
      int yMinEnc = encodeValue(y.min);
      int yMaxEnc = encodeValue(y.max);
      int zMinEnc = encodeValue(z.min);
      int zMaxEnc = encodeValue(z.max);

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " bbox: x=" + x + " (" + xMinEnc + " TO " + xMaxEnc+ ")" + " y=" + y + " (" + yMinEnc + " TO " + yMaxEnc + ")"  + " z=" + z + " (" + zMinEnc + " TO " + zMaxEnc + ")" );
      }

      DocIdSet hits = r.intersect(xMinEnc, xMaxEnc,
                                  yMinEnc, yMaxEnc,
                                  zMinEnc, zMaxEnc,

                                  new BKD3DTreeReader.ValueFilter() {

                                    @Override
                                    public boolean accept(int docID) {
                                      Point point = points.get(docID);
                                      //System.out.println("  accept docID=" + docID + " point=" + point + " (x=" + encodeValue(point.x) + " y=" + encodeValue(point.y) + " z=" + encodeValue(point.z) + ")");

                                      int xEnc = encodeValue(point.x);
                                      int yEnc = encodeValue(point.y);
                                      int zEnc = encodeValue(point.z);

                                      boolean accept = xEnc >= xMinEnc && xEnc <= xMaxEnc &&
                                        yEnc >= yMinEnc && yEnc <= yMaxEnc &&
                                        zEnc >= zMinEnc && zEnc <= zMaxEnc;
                                      //System.out.println("    " + accept);

                                      return accept;
                                    }

                                    @Override
                                    public BKD3DTreeReader.Relation compare(int cellXMin, int cellXMax,
                                                                            int cellYMin, int cellYMax,
                                                                            int cellZMin, int cellZMax) {
                                      if (cellXMin > xMaxEnc || cellXMax < xMinEnc) {
                                        return BKD3DTreeReader.Relation.OUTSIDE;
                                      }
                                      if (cellYMin > yMaxEnc || cellYMax < yMinEnc) {
                                        return BKD3DTreeReader.Relation.OUTSIDE;
                                      }
                                      if (cellZMin > zMaxEnc || cellZMax < zMinEnc) {
                                        return BKD3DTreeReader.Relation.OUTSIDE;
                                      }

                                      if (cellXMin >= xMinEnc && cellXMax <= xMaxEnc &&
                                          cellYMin >= yMinEnc && cellYMax <= yMaxEnc &&
                                          cellZMin >= zMinEnc && cellZMax <= zMaxEnc) {
                                        return BKD3DTreeReader.Relation.INSIDE;
                                      }

                                      return BKD3DTreeReader.Relation.CROSSES;
                                    }
                                  });

      DocIdSetIterator disi = hits.iterator();
      FixedBitSet matches = new FixedBitSet(numPoints);
      while (true) {
        int nextHit = disi.nextDoc();
        if (nextHit == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        matches.set(nextHit);
      }
      if (VERBOSE) {
        System.out.println("  total hits: " + matches.cardinality());
      }

      for(int docID=0;docID<numPoints;docID++) {
        Point point = points.get(docID);
        boolean actual = matches.get(docID);

        // nocommit no good the test is "using" some of the code it's supposed to test ...
        // nocommit wait: this shouldn't be necessary?  bkd tree IS precise?
        int xEnc = encodeValue(point.x);
        int yEnc = encodeValue(point.y);
        int zEnc = encodeValue(point.z);

        boolean expected = xEnc >= xMinEnc && xEnc <= xMaxEnc &&
          yEnc >= yMinEnc && yEnc <= yMaxEnc &&
          zEnc >= zMinEnc && zEnc <= zMaxEnc;

        if (expected != actual) {
          System.out.println("docID=" + docID + " is wrong: expected=" + expected + " actual=" + actual);
          System.out.println("  x=" + point.x + " (" + xEnc + ")" + " y=" + point.y + " (" + yEnc + ")" + " z=" + point.z + " (" + zEnc + ")");
          fail("wrong match");
        }
      }
    }

    in.close();
    dir.close();
  }
}

