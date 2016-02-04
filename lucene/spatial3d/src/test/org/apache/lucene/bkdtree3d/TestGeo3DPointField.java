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
package org.apache.lucene.bkdtree3d;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.geo3d.GeoArea;
import org.apache.lucene.geo3d.GeoAreaFactory;
import org.apache.lucene.geo3d.GeoBBoxFactory;
import org.apache.lucene.geo3d.GeoCircleFactory;
import org.apache.lucene.geo3d.GeoPath;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.geo3d.GeoPolygonFactory;
import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.geo3d.PlanetModel;
import org.apache.lucene.geo3d.XYZBounds;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.decodeValueCenter;
import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.decodeValueMax;
import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.decodeValueMin;
import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.encodeValue;
import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.encodeValueLenient;

public class TestGeo3DPointField extends LuceneTestCase {

  private static boolean smallBBox;
  
  @BeforeClass
  public static void beforeClass() {
    smallBBox = random().nextBoolean();
    if (VERBOSE) {
      System.err.println("TEST: smallBBox=" + smallBBox);
    }
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.alwaysDocValuesFormat(new Geo3DDocValuesFormat(PlanetModel.WGS84, maxPointsInLeaf, maxPointsSortInHeap)));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new Geo3DPointField("field", PlanetModel.WGS84, toRadians(50.7345267), toRadians(-97.5303555)));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    // We can't wrap with "exotic" readers because the query must see the BKD3DDVFormat:
    IndexSearcher s = newSearcher(r, false);
    assertEquals(1, s.search(new PointInGeo3DShapeQuery(PlanetModel.WGS84,
                                                        "field",
                                                        GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, toRadians(50), toRadians(-97), Math.PI/180.)), 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  public void testPlanetModelChanged() throws Exception {
    Directory dir = newDirectory();
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.alwaysDocValuesFormat(new Geo3DDocValuesFormat(PlanetModel.WGS84, maxPointsInLeaf, maxPointsSortInHeap)));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new Geo3DPointField("field", PlanetModel.WGS84, toRadians(50.7345267), toRadians(-97.5303555)));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = new IndexSearcher(r);
    try {
      s.search(new PointInGeo3DShapeQuery(PlanetModel.SPHERE,
                                          "field",
                                          GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, toRadians(50), toRadians(-97), Math.PI/180.)), 1);
      fail("did not hit exc");      
    } catch (IllegalStateException ise) {
      // expected
    }
    w.close();
    r.close();
    dir.close();
  }

  private static double toRadians(double degrees) {
    return Math.PI*(degrees/360.0);
  }

  public void testBKDBasic() throws Exception {
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
                                    return BKD3DTreeReader.Relation.SHAPE_INSIDE_CELL;
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

  private double randomCoord(PlanetModel planetModel) {
    return planetModel.getMaximumMagnitude() * 2*(random().nextDouble()-0.5);
  }

  private Range randomRange(PlanetModel planetModel) {
    double x = randomCoord(planetModel);
    double y = randomCoord(planetModel);
    if (x < y) {
      return new Range(x, y);
    } else {
      return new Range(y, x);
    }
  }

  private static PlanetModel getPlanetModel() {
    if (random().nextBoolean()) {
      // Use one of the earth models:
      if (random().nextBoolean()) {
        return PlanetModel.WGS84;
      } else {
        return PlanetModel.SPHERE;
      }
    } else {
      // Make a randomly squashed planet:
      double oblateness = random().nextDouble() * 0.5 - 0.25;
      return new PlanetModel(1.0 + oblateness, 1.0 - oblateness);
    }
  }

  public void testBKDRandom() throws Exception {
    final List<Point> points = new ArrayList<>();
    int numPoints = atLeast(10000);
    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048); 

    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);

    PlanetModel planetModel = getPlanetModel();
    final double planetMax = planetModel.getMaximumMagnitude();
    
    BKD3DTreeWriter w = new BKD3DTreeWriter(maxPointsInLeaf, maxPointsSortInHeap);
    for(int docID=0;docID<numPoints;docID++) {
      Point point;
      if (docID > 0 && random().nextInt(30) == 17) {
        // Dup point
        point = points.get(random().nextInt(points.size()));
      } else {
        point = new Point(randomCoord(planetModel),
                          randomCoord(planetModel),
                          randomCoord(planetModel));
      }

      if (VERBOSE) {
        System.err.println("  docID=" + docID + " point=" + point);
        System.err.println("    x=" + encodeValue(planetMax, point.x) +
                           " y=" + encodeValue(planetMax, point.y) +
                           " z=" + encodeValue(planetMax, point.z));
      }

      points.add(point);
      w.add(encodeValue(planetMax, point.x),
            encodeValue(planetMax, point.y),
            encodeValue(planetMax, point.z),
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
      Range x = randomRange(planetModel);
      Range y = randomRange(planetModel);
      Range z = randomRange(planetModel);

      final int xMinEnc = encodeValue(planetMax, x.min);
      final int xMaxEnc = encodeValue(planetMax, x.max);
      final int yMinEnc = encodeValue(planetMax, y.min);
      final int yMaxEnc = encodeValue(planetMax, y.max);
      final int zMinEnc = encodeValue(planetMax, z.min);
      final int zMaxEnc = encodeValue(planetMax, z.max);

      if (VERBOSE) {
        System.err.println("\nTEST: iter=" + iter + " bbox: x=" + x + " (" + xMinEnc + " TO " + xMaxEnc+ ")" + " y=" + y + " (" + yMinEnc + " TO " + yMaxEnc + ")"  + " z=" + z + " (" + zMinEnc + " TO " + zMaxEnc + ")" );
      }

      DocIdSet hits = r.intersect(xMinEnc, xMaxEnc,
                                  yMinEnc, yMaxEnc,
                                  zMinEnc, zMaxEnc,

                                  new BKD3DTreeReader.ValueFilter() {

                                    @Override
                                    public boolean accept(int docID) {
                                      Point point = points.get(docID);
                                      //System.out.println("  accept docID=" + docID + " point=" + point + " (x=" + encodeValue(point.x) + " y=" + encodeValue(point.y) + " z=" + encodeValue(point.z) + ")");

                                      // System.out.println("  accept docID=" + docID + " point: x=" + point.x + " y=" + point.y + " z=" + point.z);
                                      int xEnc = encodeValue(planetMax, point.x);
                                      int yEnc = encodeValue(planetMax, point.y);
                                      int zEnc = encodeValue(planetMax, point.z);

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
                                        return BKD3DTreeReader.Relation.SHAPE_OUTSIDE_CELL;
                                      }
                                      if (cellYMin > yMaxEnc || cellYMax < yMinEnc) {
                                        return BKD3DTreeReader.Relation.SHAPE_OUTSIDE_CELL;
                                      }
                                      if (cellZMin > zMaxEnc || cellZMax < zMinEnc) {
                                        return BKD3DTreeReader.Relation.SHAPE_OUTSIDE_CELL;
                                      }

                                      if (cellXMin >= xMinEnc && cellXMax <= xMaxEnc &&
                                          cellYMin >= yMinEnc && cellYMax <= yMaxEnc &&
                                          cellZMin >= zMinEnc && cellZMax <= zMaxEnc) {
                                        return BKD3DTreeReader.Relation.CELL_INSIDE_SHAPE;
                                      }

                                      if (xMinEnc >= cellXMin && xMaxEnc <= cellXMax &&
                                          yMinEnc >= cellYMin && yMaxEnc <= cellYMax &&
                                          zMinEnc >= cellZMin && zMaxEnc <= cellZMax) {
                                        return BKD3DTreeReader.Relation.SHAPE_INSIDE_CELL;
                                      }

                                      return BKD3DTreeReader.Relation.SHAPE_CROSSES_CELL;
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
        System.err.println("  total hits: " + matches.cardinality());
      }

      for(int docID=0;docID<numPoints;docID++) {
        Point point = points.get(docID);
        boolean actual = matches.get(docID);

        // We must quantize exactly as BKD tree does else we'll get false failures
        int xEnc = encodeValue(planetMax, point.x);
        int yEnc = encodeValue(planetMax, point.y);
        int zEnc = encodeValue(planetMax, point.z);

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

  private static class Cell {
    static int nextCellID;

    final Cell parent;
    final int cellID;
    final int xMinEnc, xMaxEnc;
    final int yMinEnc, yMaxEnc;
    final int zMinEnc, zMaxEnc;
    final int splitCount;

    public Cell(Cell parent,
                int xMinEnc, int xMaxEnc,
                int yMinEnc, int yMaxEnc,
                int zMinEnc, int zMaxEnc,
                int splitCount) {
      this.parent = parent;
      this.xMinEnc = xMinEnc;
      this.xMaxEnc = xMaxEnc;
      this.yMinEnc = yMinEnc;
      this.yMaxEnc = yMaxEnc;
      this.zMinEnc = zMinEnc;
      this.zMaxEnc = zMaxEnc;
      this.cellID = nextCellID++;
      this.splitCount = splitCount;
    }

    /** Returns true if the quantized point lies within this cell, inclusive on all bounds. */
    public boolean contains(double planetMax, GeoPoint point) {
      int docX = encodeValue(planetMax, point.x);
      int docY = encodeValue(planetMax, point.y);
      int docZ = encodeValue(planetMax, point.z);

      return docX >= xMinEnc && docX <= xMaxEnc &&
        docY >= yMinEnc && docY <= yMaxEnc && 
        docZ >= zMinEnc && docZ <= zMaxEnc;
    }

    @Override
    public String toString() {
      return "cell=" + cellID + (parent == null ? "" : " parentCellID=" + parent.cellID) + " x: " + xMinEnc + " TO " + xMaxEnc + ", y: " + yMinEnc + " TO " + yMaxEnc + ", z: " + zMinEnc + " TO " + zMaxEnc + ", splits: " + splitCount;
    }
  }

  private static GeoPoint quantize(double planetMax, GeoPoint point) {
    return new GeoPoint(decodeValueCenter(planetMax, encodeValue(planetMax, point.x)),
                        decodeValueCenter(planetMax, encodeValue(planetMax, point.y)),
                        decodeValueCenter(planetMax, encodeValue(planetMax, point.z)));
  }

  /** Tests consistency of GeoArea.getRelationship vs GeoShape.isWithin */
  public void testGeo3DRelations() throws Exception {

    PlanetModel planetModel = getPlanetModel();

    int numDocs = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: " + numDocs + " docs");
    }

    GeoPoint[] docs = new GeoPoint[numDocs];
    for(int docID=0;docID<numDocs;docID++) {
      docs[docID] = new GeoPoint(planetModel, toRadians(randomLat()), toRadians(randomLon()));
      if (VERBOSE) {
        System.out.println("  doc=" + docID + ": " + docs[docID]);
      }
    }

    double planetMax = planetModel.getMaximumMagnitude();

    int iters = atLeast(10);

    int recurseDepth = RandomInts.randomIntBetween(random(), 5, 15);

    iters = atLeast(50);
    
    for(int iter=0;iter<iters;iter++) {
      GeoShape shape = randomShape(planetModel);

      StringWriter sw = new StringWriter();
      PrintWriter log = new PrintWriter(sw, true);

      if (VERBOSE) {
        log.println("TEST: iter=" + iter + " shape=" + shape);
      }

      XYZBounds bounds = new XYZBounds();
      shape.getBounds(bounds);

      // Start with the root cell that fully contains the shape:
      Cell root = new Cell(null,
                           encodeValueLenient(planetMax, bounds.getMinimumX()),
                           encodeValueLenient(planetMax, bounds.getMaximumX()),
                           encodeValueLenient(planetMax, bounds.getMinimumY()),
                           encodeValueLenient(planetMax, bounds.getMaximumY()),
                           encodeValueLenient(planetMax, bounds.getMinimumZ()),
                           encodeValueLenient(planetMax, bounds.getMaximumZ()),
                           0);

      if (VERBOSE) {
        log.println("  root cell: " + root);
      }

      List<Cell> queue = new ArrayList<>();
      queue.add(root);
      Set<Integer> hits = new HashSet<>();

      while (queue.size() > 0) {
        Cell cell = queue.get(queue.size()-1);
        queue.remove(queue.size()-1);
        if (VERBOSE) {
          log.println("  cycle: " + cell + " queue.size()=" + queue.size());
        }

        if (random().nextInt(10) == 7 || cell.splitCount > recurseDepth) {
          if (VERBOSE) {
            log.println("    leaf");
          }
          // Leaf cell: brute force check all docs that fall within this cell:
          for(int docID=0;docID<numDocs;docID++) {
            GeoPoint point = docs[docID];
            if (cell.contains(planetMax, point)) {
              if (shape.isWithin(quantize(planetMax, point))) {
                if (VERBOSE) {
                  log.println("    check doc=" + docID + ": match!");
                }
                hits.add(docID);
              } else {
                if (VERBOSE) {
                  log.println("    check doc=" + docID + ": no match");
                }
              }
            }
          }
        } else {
          
          GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(planetModel,
                                                        decodeValueMin(planetMax, cell.xMinEnc), decodeValueMax(planetMax, cell.xMaxEnc),
                                                        decodeValueMin(planetMax, cell.yMinEnc), decodeValueMax(planetMax, cell.yMaxEnc),
                                                        decodeValueMin(planetMax, cell.zMinEnc), decodeValueMax(planetMax, cell.zMaxEnc));

          if (VERBOSE) {
            log.println("    minx="+decodeValueMin(planetMax, cell.xMinEnc)+" maxx="+decodeValueMax(planetMax, cell.xMaxEnc)+
              " miny="+decodeValueMin(planetMax, cell.yMinEnc)+" maxy="+decodeValueMax(planetMax, cell.yMaxEnc)+
              " minz="+decodeValueMin(planetMax, cell.zMinEnc)+" maxz="+decodeValueMax(planetMax, cell.zMaxEnc));
          }

          switch (xyzSolid.getRelationship(shape)) {          
          case GeoArea.CONTAINS:
            // Shape fully contains the cell: blindly add all docs in this cell:
            if (VERBOSE) {
              log.println("    GeoArea.CONTAINS: now addAll");
            }
            for(int docID=0;docID<numDocs;docID++) {
              if (cell.contains(planetMax, docs[docID])) {
                if (VERBOSE) {
                  log.println("    addAll doc=" + docID);
                }
                hits.add(docID);
              }
            }
            continue;
          case GeoArea.OVERLAPS:
            if (VERBOSE) {
              log.println("    GeoArea.OVERLAPS: keep splitting");
            }
            // They do overlap but neither contains the other:
            //log.println("    crosses1");
            break;
          case GeoArea.WITHIN:
            if (VERBOSE) {
              log.println("    GeoArea.WITHIN: keep splitting");
            }
            // Cell fully contains the shape:
            //log.println("    crosses2");
            break;
          case GeoArea.DISJOINT:
            // They do not overlap at all: don't recurse on this cell
            //log.println("    outside");
            if (VERBOSE) {
              log.println("    GeoArea.DISJOINT: drop this cell");
              for(int docID=0;docID<numDocs;docID++) {
                if (cell.contains(planetMax, docs[docID])) {
                  if (VERBOSE) {
                    log.println("    skip doc=" + docID);
                  }
                }
              }
            }
            continue;
          default:
            assert false;
          }

          // Randomly split:
          switch(random().nextInt(3)) {

          case 0:
            // Split on X:
            {
              int splitValue = RandomInts.randomIntBetween(random(), cell.xMinEnc, cell.xMaxEnc);
              if (VERBOSE) {
                log.println("    now split on x=" + splitValue);
              }
              Cell cell1 = new Cell(cell,
                                 cell.xMinEnc, splitValue,
                                 cell.yMinEnc, cell.yMaxEnc,
                                 cell.zMinEnc, cell.zMaxEnc,
                                 cell.splitCount+1);
              Cell cell2 = new Cell(cell,
                                 splitValue, cell.xMaxEnc,
                                 cell.yMinEnc, cell.yMaxEnc,
                                 cell.zMinEnc, cell.zMaxEnc,
                                 cell.splitCount+1);
              if (VERBOSE) {
                log.println("    split cell1: " + cell1);
                log.println("    split cell2: " + cell2);
              }
              queue.add(cell1);
              queue.add(cell2);
            }
            break;

          case 1:
            // Split on Y:
            {
              int splitValue = RandomInts.randomIntBetween(random(), cell.yMinEnc, cell.yMaxEnc);
              if (VERBOSE) {
                log.println("    now split on y=" + splitValue);
              }
              Cell cell1 = new Cell(cell,
                                 cell.xMinEnc, cell.xMaxEnc,
                                 cell.yMinEnc, splitValue,
                                 cell.zMinEnc, cell.zMaxEnc,
                                 cell.splitCount+1);
              Cell cell2 = new Cell(cell,
                                 cell.xMinEnc, cell.xMaxEnc,
                                 splitValue, cell.yMaxEnc,
                                 cell.zMinEnc, cell.zMaxEnc,
                                 cell.splitCount+1);
              if (VERBOSE) {
                log.println("    split cell1: " + cell1);
                log.println("    split cell2: " + cell2);
              }
              queue.add(cell1);
              queue.add(cell2);
            }
            break;

          case 2:
            // Split on Z:
            {
              int splitValue = RandomInts.randomIntBetween(random(), cell.zMinEnc, cell.zMaxEnc);
              if (VERBOSE) {
                log.println("    now split on z=" + splitValue);
              }
              Cell cell1 = new Cell(cell,
                                 cell.xMinEnc, cell.xMaxEnc,
                                 cell.yMinEnc, cell.yMaxEnc,
                                 cell.zMinEnc, splitValue,
                                 cell.splitCount+1);
              Cell cell2 = new Cell(cell,
                                 cell.xMinEnc, cell.xMaxEnc,
                                 cell.yMinEnc, cell.yMaxEnc,
                                 splitValue, cell.zMaxEnc,
                                 cell.splitCount+1);
              if (VERBOSE) {
                log.println("    split cell1: " + cell1);
                log.println("    split cell2: " + cell2);
              }
              queue.add(cell1);
              queue.add(cell2);
            }
            break;
          }
        }
      }

      if (VERBOSE) {
        log.println("  " + hits.size() + " hits");
      }

      // Done matching, now verify:
      boolean fail = false;
      for(int docID=0;docID<numDocs;docID++) {
        GeoPoint point = docs[docID];
        GeoPoint quantized = quantize(planetMax, point);
        boolean expected = shape.isWithin(quantized);

        if (expected != shape.isWithin(point)) {
          // Quantization changed the result; skip testing this doc:
          continue;
        }

        boolean actual = hits.contains(docID);
        if (actual != expected) {
          if (actual) {
            log.println("doc=" + docID + " matched but should not");
          } else {
            log.println("doc=" + docID + " did not match but should");
          }
          log.println("  point=" + docs[docID]);
          log.println("  quantized=" + quantize(planetMax, docs[docID]));
          fail = true;
        }
      }

      if (fail) {
        System.out.print(sw.toString());
        fail("invalid hits for shape=" + shape);
      }
    }
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  public void testRandomMedium() throws Exception {
    doTestRandom(10000);
  }

  @Nightly
  public void testRandomBig() throws Exception {
    doTestRandom(200000);
  }

  private void doTestRandom(int count) throws Exception {
    int numPoints = atLeast(count);

    if (VERBOSE) {
      System.err.println("TEST: numPoints=" + numPoints);
    }

    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];

    boolean haveRealDoc = false;

    for (int docID=0;docID<numPoints;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        lats[docID] = Double.NaN;
        if (VERBOSE) {
          System.err.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x < 3 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (Double.isNaN(lats[oldDocID]) == false) {
            break;
          }
        }
            
        if (x == 0) {
          // Identical lat to old point
          lats[docID] = lats[oldDocID];
          lons[docID] = toRadians(randomLon());
          if (VERBOSE) {
            System.err.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat as doc=" + oldDocID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[docID] = toRadians(randomLat());
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            System.err.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lon as doc=" + oldDocID + ")");
          }
        } else {
          assert x == 2;
          // Fully identical point:
          lats[docID] = lats[oldDocID];
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            System.err.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat/lon as doc=" + oldDocID + ")");
          }
        }
      } else {
        lats[docID] = toRadians(randomLat());
        lons[docID] = toRadians(randomLon());
        haveRealDoc = true;
        if (VERBOSE) {
          System.err.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID]);
        }
      }
    }

    verify(lats, lons);
  }

  private static double randomLat() {
    if (smallBBox) {
      return 2.0 * (random().nextDouble()-0.5);
    } else {
      return -90 + 180.0 * random().nextDouble();
    }
  }

  private static double randomLon() {
    if (smallBBox) {
      return 2.0 * (random().nextDouble()-0.5);
    } else {
      return -180 + 360.0 * random().nextDouble();
    }
  }

  // Poached from Geo3dRptTest.randomShape:
  private static GeoShape randomShape(PlanetModel planetModel) {
    while (true) {
      final int shapeType = random().nextInt(4);
      switch (shapeType) {
      case 0: {
        // Polygons
        final int vertexCount = random().nextInt(3) + 3;
        final List<GeoPoint> geoPoints = new ArrayList<>();
        while (geoPoints.size() < vertexCount) {
          final GeoPoint gPt = new GeoPoint(planetModel, toRadians(randomLat()), toRadians(randomLon()));
          geoPoints.add(gPt);
        }
        final int convexPointIndex = random().nextInt(vertexCount);       //If we get this wrong, hopefully we get IllegalArgumentException
        try {
          return GeoPolygonFactory.makeGeoPolygon(planetModel, geoPoints, convexPointIndex);
        } catch (IllegalArgumentException e) {
          // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
          // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
          continue;
        }
      }

      case 1: {
        // Circles

        double lat = toRadians(randomLat());
        double lon = toRadians(randomLon());

        double angle;
        if (smallBBox) {
          angle = random().nextDouble() * Math.PI/360.0;
        } else {
          angle = random().nextDouble() * Math.PI/2.0;
        }

        try {
          return GeoCircleFactory.makeGeoCircle(planetModel, lat, lon, angle);
        } catch (IllegalArgumentException iae) {
          // angle is too small; try again:
          continue;
        }
      }

      case 2: {
        // Rectangles
        double lat0 = toRadians(randomLat());
        double lat1 = toRadians(randomLat());
        if (lat1 < lat0) {
          double x = lat0;
          lat0 = lat1;
          lat1 = x;
        }
        double lon0 = toRadians(randomLon());
        double lon1 = toRadians(randomLon());
        if (lon1 < lon0) {
          double x = lon0;
          lon0 = lon1;
          lon1 = x;
        }

        return GeoBBoxFactory.makeGeoBBox(planetModel, lat1, lat0, lon0, lon1);
      }

      case 3: {
        // Paths
        final int pointCount = random().nextInt(5) + 1;
        final double width = toRadians(random().nextInt(89)+1);
        try {
          final GeoPath path = new GeoPath(planetModel, width);
          for (int i = 0; i < pointCount; i++) {
            path.addPoint(toRadians(randomLat()), toRadians(randomLon()));
          }
          path.done();
          return path;
        } catch (IllegalArgumentException e) {
          // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
          // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
          continue;
        }
      }

      default:
        throw new IllegalStateException("Unexpected shape type");
      }
    }
  }

  private static void verify(final double[] lats, final double[] lons) throws Exception {
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    IndexWriterConfig iwc = newIndexWriterConfig();

    final PlanetModel planetModel = getPlanetModel();

    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < lats.length/100) {
      iwc.setMaxBufferedDocs(lats.length/100);
    }
    final DocValuesFormat dvFormat = new Geo3DDocValuesFormat(planetModel, maxPointsInLeaf, maxPointsSortInHeap);
    Codec codec = new Lucene54Codec() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          if (field.equals("point")) {
            return dvFormat;
          } else {
            return super.getDocValuesFormatForField(field);
          }
        }
      };
    iwc.setCodec(codec);
    Directory dir;
    if (lats.length > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
    } else {
      dir = newDirectory();
    }
    final Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int id=0;id<lats.length;id++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (Double.isNaN(lats[id]) == false) {
        doc.add(new Geo3DPointField("point", planetModel, lats[id], lons[id]));
      }
      w.addDocument(doc);
      if (id > 0 && random().nextInt(100) == 42) {
        int idToDelete = random().nextInt(id);
        w.deleteDocuments(new Term("id", ""+idToDelete));
        deleted.add(idToDelete);
        if (VERBOSE) {
          System.err.println("  delete id=" + idToDelete);
        }
      }
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    final IndexReader r = DirectoryReader.open(w, true);
    w.close();

    // We can't wrap with "exotic" readers because the geo3d query must see the Geo3DDVFormat:
    final IndexSearcher s = newSearcher(r, false);

    int numThreads = TestUtil.nextInt(random(), 2, 5);

    List<Thread> threads = new ArrayList<>();
    final int iters = atLeast(100);

    final CountDownLatch startingGun = new CountDownLatch(1);
    final AtomicBoolean failed = new AtomicBoolean();

    for(int i=0;i<numThreads;i++) {
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              _run();
            } catch (Exception e) {
              failed.set(true);
              throw new RuntimeException(e);
            }
          }

          private void _run() throws Exception {
            startingGun.await();

            NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

            for (int iter=0;iter<iters && failed.get() == false;iter++) {

              GeoShape shape = randomShape(planetModel);

              if (VERBOSE) {
                System.err.println("\n" + Thread.currentThread() + ": TEST: iter=" + iter + " shape="+shape);
              }
              
              Query query = new PointInGeo3DShapeQuery(planetModel, "point", shape);

              if (VERBOSE) {
                System.err.println("  using query: " + query);
              }

              final FixedBitSet hits = new FixedBitSet(r.maxDoc());

              s.search(query, new SimpleCollector() {

                  private int docBase;

                  @Override
                  public boolean needsScores() {
                    return false;
                  }

                  @Override
                  protected void doSetNextReader(LeafReaderContext context) throws IOException {
                    docBase = context.docBase;
                  }

                  @Override
                  public void collect(int doc) {
                    hits.set(docBase+doc);
                  }
                });

              if (VERBOSE) {
                System.err.println("  hitCount: " + hits.cardinality());
              }
      
              for(int docID=0;docID<r.maxDoc();docID++) {
                int id = (int) docIDToID.get(docID);
                if (Double.isNaN(lats[id]) == false) {

                  // Accurate point:
                  GeoPoint point1 = new GeoPoint(planetModel, lats[id], lons[id]);

                  // Quantized point (32 bits per dim):
                  GeoPoint point2 = quantize(planetModel.getMaximumMagnitude(), point1);

                  if (shape.isWithin(point1) != shape.isWithin(point2)) {
                    if (VERBOSE) {
                      System.out.println("  skip checking docID=" + docID + " quantization changed the expected result from " + shape.isWithin(point1) + " to " + shape.isWithin(point2));
                    }
                    continue;
                  }

                  boolean expected = ((deleted.contains(id) == false) && shape.isWithin(point2));
                  if (hits.get(docID) != expected) {
                    fail(Thread.currentThread().getName() + ": iter=" + iter + " id=" + id + " docID=" + docID + " lat=" + lats[id] + " lon=" + lons[id] + " expected " + expected + " but got: " + hits.get(docID) + " deleted?=" + deleted.contains(id) + "\n  point1=" + point1 + ", iswithin="+shape.isWithin(point1)+"\n  point2=" + point2 + ", iswithin="+shape.isWithin(point2) + "\n  query=" + query);
                  }
                } else {
                  assertFalse(hits.get(docID));
                }

              }
            }
          }
        };
      thread.setName("T" + i);
      thread.start();
      threads.add(thread);
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }
    IOUtils.close(r, dir);
  }
}
