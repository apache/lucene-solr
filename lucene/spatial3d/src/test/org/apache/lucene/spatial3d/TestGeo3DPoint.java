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
package org.apache.lucene.spatial3d;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene60.Lucene60PointsReader;
import org.apache.lucene.codecs.lucene60.Lucene60PointsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoAreaFactory;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoCircleFactory;
import org.apache.lucene.spatial3d.geom.GeoPathFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.Plane;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.SidedPlane;
import org.apache.lucene.spatial3d.geom.XYZBounds;
import org.apache.lucene.spatial3d.geom.XYZSolid;
import org.apache.lucene.spatial3d.geom.XYZSolidFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public class TestGeo3DPoint extends LuceneTestCase {

  private static Codec getCodec() {
    if (Codec.getDefault().getName().equals("Lucene62")) {
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 16, 2048);
      double maxMBSortInHeap = 3.0 + (3*random().nextDouble());
      if (VERBOSE) {
        System.out.println("TEST: using Lucene60PointsFormat with maxPointsInLeafNode=" + maxPointsInLeafNode + " and maxMBSortInHeap=" + maxMBSortInHeap);
      }

      return new FilterCodec("Lucene62", Codec.getDefault()) {
        @Override
        public PointsFormat pointsFormat() {
          return new PointsFormat() {
            @Override
            public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
              return new Lucene60PointsWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
            }

            @Override
            public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
              return new Lucene60PointsReader(readState);
            }
          };
        }
      };
    } else {
      return Codec.getDefault();
    }
  }

  public void testBasic() throws Exception {
    Directory dir = getDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new Geo3DPoint("field", 50.7345267, -97.5303555));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    // We can't wrap with "exotic" readers because the query must see the BKD3DDVFormat:
    IndexSearcher s = newSearcher(r, false);
    assertEquals(1, s.search(Geo3DPoint.newShapeQuery("field",
                                                      GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, toRadians(50), toRadians(-97), Math.PI/180.)), 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  private static double toRadians(double degrees) {
    return degrees * Geo3DUtil.RADIANS_PER_DEGREE;
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
    public boolean contains(GeoPoint point) {
      int docX = Geo3DUtil.encodeValue(point.x);
      int docY = Geo3DUtil.encodeValue(point.y);
      int docZ = Geo3DUtil.encodeValue(point.z);

      return docX >= xMinEnc && docX <= xMaxEnc &&
        docY >= yMinEnc && docY <= yMaxEnc && 
        docZ >= zMinEnc && docZ <= zMaxEnc;
    }

    @Override
    public String toString() {
      return "cell=" + cellID + (parent == null ? "" : " parentCellID=" + parent.cellID) + " x: " + xMinEnc + " TO " + xMaxEnc + ", y: " + yMinEnc + " TO " + yMaxEnc + ", z: " + zMinEnc + " TO " + zMaxEnc + ", splits: " + splitCount;
    }
  }

  private static double quantize(double xyzValue) {
    return Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(xyzValue));
  }

  private static GeoPoint quantize(GeoPoint point) {
    return new GeoPoint(quantize(point.x), quantize(point.y), quantize(point.z));
  }

  /** Tests consistency of GeoArea.getRelationship vs GeoShape.isWithin */
  public void testGeo3DRelations() throws Exception {

    int numDocs = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: " + numDocs + " docs");
    }

    GeoPoint[] docs = new GeoPoint[numDocs];
    GeoPoint[] unquantizedDocs = new GeoPoint[numDocs];
    for(int docID=0;docID<numDocs;docID++) {
      unquantizedDocs[docID] = new GeoPoint(PlanetModel.WGS84, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude()));
      docs[docID] = quantize(unquantizedDocs[docID]);
      if (VERBOSE) {
        System.out.println("  doc=" + docID + ": " + docs[docID] + "; unquantized: "+unquantizedDocs[docID]);
      }
    }

    int iters = atLeast(10);

    int recurseDepth = RandomNumbers.randomIntBetween(random(), 5, 15);

    iters = atLeast(50);
    
    for(int iter=0;iter<iters;iter++) {
      GeoShape shape = randomShape();

      StringWriter sw = new StringWriter();
      PrintWriter log = new PrintWriter(sw, true);

      if (VERBOSE) {
        log.println("TEST: iter=" + iter + " shape=" + shape);
      }

      XYZBounds bounds = new XYZBounds();
      shape.getBounds(bounds);

      // Start with the root cell that fully contains the shape:
      Cell root = new Cell(null,
                           encodeValueLenient(bounds.getMinimumX()),
                           encodeValueLenient(bounds.getMaximumX()),
                           encodeValueLenient(bounds.getMinimumY()),
                           encodeValueLenient(bounds.getMaximumY()),
                           encodeValueLenient(bounds.getMinimumZ()),
                           encodeValueLenient(bounds.getMaximumZ()),
                           0);

      if (VERBOSE) {
        log.println("  root cell: " + root);
      }

      // make sure the root cell (XYZBounds) does in fact contain all points that the shape contains
      {
        boolean fail = false;
        for(int docID=0;docID<numDocs;docID++) {
          if (root.contains(docs[docID]) == false) {
            boolean expected = shape.isWithin(unquantizedDocs[docID]);
            if (expected) {
              log.println("    doc=" + docID + " is contained by shape but is outside the returned XYZBounds");
              log.println("      unquantized=" + unquantizedDocs[docID]);
              log.println("      quantized=" + docs[docID]);
              fail = true;
            }
          }
        }

        if (fail) {
          log.println("  shape=" + shape);
          log.println("  bounds=" + bounds);
          System.out.print(sw.toString());
          fail("invalid bounds for shape=" + shape);
        }
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
            GeoPoint mappedPoint = unquantizedDocs[docID];
            boolean pointWithinShape = shape.isWithin(point);
            boolean mappedPointWithinShape = shape.isWithin(mappedPoint);
            if (cell.contains(point)) {
              if (mappedPointWithinShape) {
                if (VERBOSE) {
                  log.println("    check doc=" + docID + ": match!  Actual quantized point within: "+pointWithinShape);
                }
                hits.add(docID);
              } else {
                if (VERBOSE) {
                  log.println("    check doc=" + docID + ": no match.  Quantized point within: "+pointWithinShape);
                }
              }
            }
          }
        } else {
          
          GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
                                                        Geo3DUtil.decodeValueFloor(cell.xMinEnc), Geo3DUtil.decodeValueCeil(cell.xMaxEnc),
                                                        Geo3DUtil.decodeValueFloor(cell.yMinEnc), Geo3DUtil.decodeValueCeil(cell.yMaxEnc),
                                                        Geo3DUtil.decodeValueFloor(cell.zMinEnc), Geo3DUtil.decodeValueCeil(cell.zMaxEnc));

          if (VERBOSE) {
            log.println("    minx="+Geo3DUtil.decodeValueFloor(cell.xMinEnc)+" maxx="+Geo3DUtil.decodeValueCeil(cell.xMaxEnc)+
              " miny="+Geo3DUtil.decodeValueFloor(cell.yMinEnc)+" maxy="+Geo3DUtil.decodeValueCeil(cell.yMaxEnc)+
              " minz="+Geo3DUtil.decodeValueFloor(cell.zMinEnc)+" maxz="+Geo3DUtil.decodeValueCeil(cell.zMaxEnc));
          }

          switch (xyzSolid.getRelationship(shape)) {          
          case GeoArea.CONTAINS:
            // Shape fully contains the cell: blindly add all docs in this cell:
            if (VERBOSE) {
              log.println("    GeoArea.CONTAINS: now addAll");
            }
            for(int docID=0;docID<numDocs;docID++) {
              if (cell.contains(docs[docID])) {
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
                if (cell.contains(docs[docID])) {
                  log.println("    skip doc=" + docID);
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
              int splitValue = RandomNumbers.randomIntBetween(random(), cell.xMinEnc, cell.xMaxEnc);
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
              int splitValue = RandomNumbers.randomIntBetween(random(), cell.yMinEnc, cell.yMaxEnc);
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
              int splitValue = RandomNumbers.randomIntBetween(random(), cell.zMinEnc, cell.zMaxEnc);
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
        GeoPoint mappedPoint = unquantizedDocs[docID];
        boolean expected = shape.isWithin(mappedPoint);
        boolean actual = hits.contains(docID);
        if (actual != expected) {
          if (actual) {
            log.println("doc=" + docID + " should not have matched but did");
          } else {
            log.println("doc=" + docID + " should match but did not");
          }
          log.println("  point=" + point);
          log.println("  mappedPoint=" + mappedPoint);
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
          lons[docID] = GeoTestUtil.nextLongitude();
          if (VERBOSE) {
            System.err.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat as doc=" + oldDocID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[docID] = GeoTestUtil.nextLatitude();
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
        lats[docID] = GeoTestUtil.nextLatitude();
        lons[docID] = GeoTestUtil.nextLongitude();
        haveRealDoc = true;
        if (VERBOSE) {
          System.err.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID]);
        }
      }
    }

    verify(lats, lons);
  }

  public void testPolygonOrdering() {
    final double[] lats = new double[] {
      51.204382859999996, 50.89947531437482, 50.8093624806861,50.8093624806861, 50.89947531437482, 51.204382859999996, 51.51015366140113, 51.59953838204167, 51.59953838204167, 51.51015366140113, 51.204382859999996};
    final double[] lons = new double[] {
      0.8747711978759765, 0.6509219832137298, 0.35960265165247807, 0.10290284834752167, -0.18841648321373008, -0.41226569787597667, -0.18960465285650027, 0.10285893781346236, 0.35964656218653757, 0.6521101528565002, 0.8747711978759765};
    final Query q = Geo3DPoint.newPolygonQuery("point", new Polygon(lats, lons));
    //System.out.println(q);
    assertTrue(!q.toString().contains("GeoConcavePolygon"));
  }
  
  private static final double MEAN_EARTH_RADIUS_METERS = PlanetModel.WGS84_MEAN;
  
  private static Query random3DQuery(final String field) {
    while (true) {
      final int shapeType = random().nextInt(5);
      switch (shapeType) {
      case 4: {
        // Large polygons
        final boolean isClockwise = random().nextDouble() < 0.5;
        try {
          final Query q = Geo3DPoint.newLargePolygonQuery(field, makePoly(PlanetModel.WGS84,
            new GeoPoint(PlanetModel.WGS84, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude())),
            isClockwise,
            true));
          //System.err.println("Generated: "+q);
          //assertTrue(false);
          return q;
        } catch (IllegalArgumentException e) {
          continue;
        }
      }
      
      case 0: {
        // Polygons
        final boolean isClockwise = random().nextDouble() < 0.5;
        try {
          final Query q = Geo3DPoint.newPolygonQuery(field, makePoly(PlanetModel.WGS84,
            new GeoPoint(PlanetModel.WGS84, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude())),
            isClockwise,
            true));
          //System.err.println("Generated: "+q);
          //assertTrue(false);
          return q;
        } catch (IllegalArgumentException e) {
          continue;
        }
      }

      case 1: {
        // Circles
        final double widthMeters = random().nextDouble() * Math.PI * MEAN_EARTH_RADIUS_METERS;
        try {
          return Geo3DPoint.newDistanceQuery(field, GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude(), widthMeters);
        } catch (IllegalArgumentException e) {
          continue;
        }
      }

      case 2: {
        // Rectangles
        final Rectangle r = GeoTestUtil.nextBox();
        try {
          return Geo3DPoint.newBoxQuery(field, r.minLat, r.maxLat, r.minLon, r.maxLon);
        } catch (IllegalArgumentException e) {
          continue;
        }
      }

      case 3: {
        // Paths
        // TBD: Need to rework generation to be realistic
        final int pointCount = random().nextInt(5) + 1;
        final double width = random().nextDouble() * Math.PI * 0.5 * MEAN_EARTH_RADIUS_METERS;
        final double[] latitudes = new double[pointCount];
        final double[] longitudes = new double[pointCount];
        for (int i = 0; i < pointCount; i++) {
          latitudes[i] = GeoTestUtil.nextLatitude();
          longitudes[i] = GeoTestUtil.nextLongitude();
        }
        try {
          return Geo3DPoint.newPathQuery(field, latitudes, longitudes, width);
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
  
  // Poached from Geo3dRptTest.randomShape:
  private static GeoShape randomShape() {
    while (true) {
      final int shapeType = random().nextInt(4);
      switch (shapeType) {
      case 0: {
        // Polygons
        final int vertexCount = random().nextInt(3) + 3;
        final List<GeoPoint> geoPoints = new ArrayList<>();
        while (geoPoints.size() < vertexCount) {
          final GeoPoint gPt = new GeoPoint(PlanetModel.WGS84, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude()));
          geoPoints.add(gPt);
        }
        try {
          final GeoShape rval = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, geoPoints);
          if (rval == null) {
            // Degenerate polygon
            continue;
          }
          return rval;
        } catch (IllegalArgumentException e) {
          // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
          // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
          continue;
        }
      }

      case 1: {
        // Circles

        double lat = toRadians(GeoTestUtil.nextLatitude());
        double lon = toRadians(GeoTestUtil.nextLongitude());

        double angle = random().nextDouble() * Math.PI/2.0;

        try {
          return GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, lat, lon, angle);
        } catch (IllegalArgumentException iae) {
          // angle is too small; try again:
          continue;
        }
      }

      case 2: {
        // Rectangles
        double lat0 = toRadians(GeoTestUtil.nextLatitude());
        double lat1 = toRadians(GeoTestUtil.nextLatitude());
        if (lat1 < lat0) {
          double x = lat0;
          lat0 = lat1;
          lat1 = x;
        }
        double lon0 = toRadians(GeoTestUtil.nextLongitude());
        double lon1 = toRadians(GeoTestUtil.nextLongitude());
        if (lon1 < lon0) {
          double x = lon0;
          lon0 = lon1;
          lon1 = x;
        }

        return GeoBBoxFactory.makeGeoBBox(PlanetModel.WGS84, lat1, lat0, lon0, lon1);
      }

      case 3: {
        // Paths
        final int pointCount = random().nextInt(5) + 1;
        final double width = toRadians(random().nextInt(89)+1);
        final GeoPoint[] points = new GeoPoint[pointCount];
        for (int i = 0; i < pointCount; i++) {
          points[i] = new GeoPoint(PlanetModel.WGS84, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude()));
        }
        try {
          return GeoPathFactory.makeGeoPath(PlanetModel.WGS84, width, points);
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

  private static void verify(double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();

    GeoPoint[] points = new GeoPoint[lats.length];
    GeoPoint[] unquantizedPoints = new GeoPoint[lats.length];
    
    // Pre-quantize all lat/lons:
    for(int i=0;i<lats.length;i++) {
      if (Double.isNaN(lats[i]) == false) {
        //System.out.println("lats[" + i + "] = " + lats[i]);
        unquantizedPoints[i] = new GeoPoint(PlanetModel.WGS84, toRadians(lats[i]), toRadians(lons[i]));
        points[i] = quantize(unquantizedPoints[i]);
      }
    }

    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < points.length/100) {
      iwc.setMaxBufferedDocs(points.length/100);
    }
    iwc.setCodec(getCodec());
    Directory dir;
    if (points.length > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
    } else {
      dir = getDirectory();
    }
    Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int id=0;id<points.length;id++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      GeoPoint point = points[id];
      if (point != null) {
        doc.add(new Geo3DPoint("point", point.x, point.y, point.z));
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
    final IndexReader r = DirectoryReader.open(w);
    if (VERBOSE) {
      System.out.println("TEST: using reader " + r);
    }
    w.close();

    // We can't wrap with "exotic" readers because the geo3d query must see the Geo3DDVFormat:
    IndexSearcher s = newSearcher(r, false);

    final int iters = atLeast(100);

    NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

    for (int iter=0;iter<iters;iter++) {

      /*
      GeoShape shape = randomShape();

      if (VERBOSE) {
        System.err.println("\nTEST: iter=" + iter + " shape="+shape);
      }
      */
      
      Query query = random3DQuery("point"); // Geo3DPoint.newShapeQuery("point", shape);

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
        GeoPoint point = points[id];
        GeoPoint unquantizedPoint = unquantizedPoints[id];
        if (point != null && unquantizedPoint != null) {
          GeoShape shape = ((PointInGeo3DShapeQuery)query).getShape();
          XYZBounds bounds = new XYZBounds();
          shape.getBounds(bounds);
          XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds.getMinimumX(), bounds.getMaximumX(), bounds.getMinimumY(), bounds.getMaximumY(), bounds.getMinimumZ(), bounds.getMaximumZ());

          boolean expected = ((deleted.contains(id) == false) && shape.isWithin(point));
          if (hits.get(docID) != expected) {
            StringBuilder b = new StringBuilder();
            if (expected) {
              b.append("FAIL: id=" + id + " should have matched but did not\n");
            } else {
              b.append("FAIL: id=" + id + " should not have matched but did\n");
            }
            b.append("  shape=" + shape + "\n");
            b.append("  bounds=" + bounds + "\n");
            b.append("  world bounds=(" +
              " minX=" + PlanetModel.WGS84.getMinimumXValue() + " maxX=" + PlanetModel.WGS84.getMaximumXValue() +
              " minY=" + PlanetModel.WGS84.getMinimumYValue() + " maxY=" + PlanetModel.WGS84.getMaximumYValue() +
              " minZ=" + PlanetModel.WGS84.getMinimumZValue() + " maxZ=" + PlanetModel.WGS84.getMaximumZValue() + "\n");
            b.append("  quantized point=" + point + " within shape? "+shape.isWithin(point)+" within bounds? "+solid.isWithin(point)+"\n");
            b.append("  unquantized point=" + unquantizedPoint + " within shape? "+shape.isWithin(unquantizedPoint)+" within bounds? "+solid.isWithin(unquantizedPoint)+"\n");
            b.append("  docID=" + docID + " deleted?=" + deleted.contains(id) + "\n");
            b.append("  query=" + query + "\n");
            b.append("  explanation:\n    " + explain("point", shape, point, unquantizedPoint, r, docID).replace("\n", "\n  "));
            fail(b.toString());
          }
        } else {
          assertFalse(hits.get(docID));
        }
      }
    }

    IOUtils.close(r, dir);
  }

  public void testToString() {
    // Don't compare entire strings because Java 9 and Java 8 have slightly different values
    Geo3DPoint point = new Geo3DPoint("point", 44.244272, 7.769736);
    final String stringToCompare = "Geo3DPoint <point: x=";
    assertEquals(stringToCompare, point.toString().substring(0,stringToCompare.length()));
  }

  public void testShapeQueryToString() {
    // Don't compare entire strings because Java 9 and Java 8 have slightly different values
    final String stringToCompare = "PointInGeo3DShapeQuery: field=point: Shape: GeoStandardCircle: {planetmodel=PlanetModel.WGS84, center=[lat=0.7";
    assertEquals(stringToCompare,
      Geo3DPoint.newShapeQuery("point", GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, toRadians(44.244272), toRadians(7.769736), 0.1)).toString().substring(0,stringToCompare.length()));
  }

  private static Directory getDirectory() {     
    return newDirectory();
  }

  public void testEquals() {
    GeoShape shape = randomShape();
    Query q = Geo3DPoint.newShapeQuery("point", shape);
    assertEquals(q, Geo3DPoint.newShapeQuery("point", shape));
    assertFalse(q.equals(Geo3DPoint.newShapeQuery("point2", shape)));
    
    // make a different random shape:
    GeoShape shape2;
    do {
      shape2 = randomShape();
    } while (shape.equals(shape2));

    assertFalse(q.equals(Geo3DPoint.newShapeQuery("point", shape2)));
  }
  
  public void testComplexPolygons() {
    final PlanetModel pm = PlanetModel.WGS84;
    // Pick a random pole
    final GeoPoint randomPole = new GeoPoint(pm, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude()));
    int iters = atLeast(100);
    for (int i = 0; i < iters; i++) {
      // Create a polygon that's less than 180 degrees
      final Polygon clockWise = makePoly(pm, randomPole, true, true);
    }
    iters = atLeast(100);
    for (int i = 0; i < iters; i++) {
      // Create a polygon that's greater than 180 degrees
      final Polygon counterClockWise = makePoly(pm, randomPole, false, true);
    }
  }

  protected static double MINIMUM_EDGE_ANGLE = toRadians(5.0);
  protected static double MINIMUM_ARC_ANGLE = toRadians(1.0);
  
  /** Cook up a random Polygon that makes sense, with possible nested polygon within.
    * This is part of testing more complex polygons with nested holes.  Picking random points
    * doesn't do it because it's almost impossible to come up with nested ones of the proper 
    * clockwise/counterclockwise rotation that way.
    */
  protected static Polygon makePoly(final PlanetModel pm, final GeoPoint pole, final boolean clockwiseDesired, final boolean createHoles) {
    
    // Polygon edges will be arranged around the provided pole, and holes will each have a pole selected within the parent
    // polygon.
    final int pointCount = TestUtil.nextInt(random(), 3, 10);
    // The point angles we pick next.  The only requirement is that they are not all on one side of the pole.
    // We arrange that by picking the next point within what's left of the remaining angle, but never more than 180 degrees,
    // and never less than what is needed to insure that the remaining point choices are less than 180 degrees always.
    // These are all picked in the context of the pole,
    final double[] angles = new double[pointCount];
    final double[] arcDistance = new double[pointCount];
    // Pick a set of points
    while (true) {
      double accumulatedAngle = 0.0;
      for (int i = 0; i < pointCount; i++) {
        final int remainingEdgeCount = pointCount - i;
        final double remainingAngle = 2.0 * Math.PI - accumulatedAngle;
        if (remainingEdgeCount == 1) {
          angles[i] = remainingAngle;
        } else {
          // The maximum angle is 180 degrees, or what's left when you give a minimal amount to each edge.
          double maximumAngle = remainingAngle - (remainingEdgeCount-1) * MINIMUM_EDGE_ANGLE;
          if (maximumAngle > Math.PI) {
            maximumAngle = Math.PI;
          }
          // The minimum angle is MINIMUM_EDGE_ANGLE, or enough to be sure nobody afterwards needs more than
          // 180 degrees.  And since we have three points to start with, we already know that.
          final double minimumAngle = MINIMUM_EDGE_ANGLE;
          // Pick the angle
          final double angle = random().nextDouble() * (maximumAngle - minimumAngle) + minimumAngle;
          angles[i] = angle;
          accumulatedAngle += angle;
        }
        // Pick the arc distance randomly; not quite the full range though
        arcDistance[i] = random().nextDouble() * (Math.PI * 0.5 - MINIMUM_ARC_ANGLE) + MINIMUM_ARC_ANGLE;
      }
      if (clockwiseDesired) {
        // Reverse the signs
        for (int i = 0; i < pointCount; i++) {
          angles[i] = -angles[i];
        }
      }
      
      // Now, use the pole's information plus angles and arcs to create GeoPoints in the right order.
      final List<GeoPoint> polyPoints = convertToPoints(pm, pole, angles, arcDistance);
      
      // Next, do some holes.  No more than 2 of these.  The poles for holes must always be within the polygon, so we're
      // going to use Geo3D to help us select those given the points we just made.
      
      final int holeCount = createHoles?TestUtil.nextInt(random(), 0, 2):0;
      
      final List<Polygon> holeList = new ArrayList<>();
      
      /* Hole logic is broken and needs rethinking
      
      // Create the geo3d polygon, so we can test out our poles.
      final GeoPolygon poly;
      try {
        poly = GeoPolygonFactory.makeGeoPolygon(pm, polyPoints, null);
      } catch (IllegalArgumentException e) {
        // This is what happens when three adjacent points are colinear, so try again.
        continue;
      }
            
      for (int i = 0; i < holeCount; i++) {
        // Choose a pole.  The poly has to be within the polygon, but it also cannot be on the polygon edge.
        // If we can't find a good pole we have to give it up and not do the hole.
        for (int k = 0; k < 500; k++) {
          final GeoPoint poleChoice = new GeoPoint(pm, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude()));
          if (!poly.isWithin(poleChoice)) {
            continue;
          }
          // We have a pole within the polygon.  Now try 100 times to build a polygon that does not intersect the outside ring.
          // After that we give up and pick a new pole.
          boolean foundOne = false;
          for (int j = 0; j < 100; j++) {
            final Polygon insidePoly = makePoly(pm, poleChoice, !clockwiseDesired, false);
            // Verify that the inside polygon is OK.  If not, discard and repeat.
            if (!verifyPolygon(pm, insidePoly, poly)) {
              continue;
            }
            holeList.add(insidePoly);
            foundOne = true;
          }
          if (foundOne) {
            break;
          }
        }
      }
      */
      
      final Polygon[] holes = holeList.toArray(new Polygon[0]);
      
      // Finally, build the polygon and return it
      final double[] lats = new double[polyPoints.size() + 1];
      final double[] lons = new double[polyPoints.size() + 1];
        
      for (int i = 0; i < polyPoints.size(); i++) {
        lats[i] = polyPoints.get(i).getLatitude() * 180.0 / Math.PI;
        lons[i] = polyPoints.get(i).getLongitude() * 180.0 / Math.PI;
      }
      lats[polyPoints.size()] = lats[0];
      lons[polyPoints.size()] = lons[0];
      return new Polygon(lats, lons, holes);
    }
  }
  
  protected static List<GeoPoint> convertToPoints(final PlanetModel pm, final GeoPoint pole, final double[] angles, final double[] arcDistances) {
    // To do the point rotations, we need the sine and cosine of the pole latitude and longitude.  Get it here for performance.
    final double sinLatitude = Math.sin(pole.getLatitude());
    final double cosLatitude = Math.cos(pole.getLatitude());
    final double sinLongitude = Math.sin(pole.getLongitude());
    final double cosLongitude = Math.cos(pole.getLongitude());
    final List<GeoPoint> rval = new ArrayList<>();
    for (int i = 0; i < angles.length; i++) {
      rval.add(createPoint(pm, angles[i], arcDistances[i], sinLatitude, cosLatitude, sinLongitude, cosLongitude));
    }
    return rval;
  }
  
  protected static GeoPoint createPoint(final PlanetModel pm,
    final double angle,
    final double arcDistance,
    final double sinLatitude,
    final double cosLatitude,
    final double sinLongitude,
    final double cosLongitude) {
    // From the angle and arc distance, convert to (x,y,z) in unit space.
    // We want the perspective to be looking down the x axis.  The "angle" measurement is thus in the Y-Z plane.
    // The arcdistance is in X.
    final double x = Math.cos(arcDistance);
    final double yzScale = Math.sin(arcDistance);
    final double y = Math.cos(angle) * yzScale;
    final double z = Math.sin(angle) * yzScale;
    // Now, rotate coordinates so that we shift everything from pole = x-axis to actual coordinates.
    // This transformation should take the point (1,0,0) and transform it to the pole's actual (x,y,z) coordinates.
    // Coordinate rotation formula:
    // x1 = x0 cos T - y0 sin T
    // y1 = x0 sin T + y0 cos T
    // We're in essence undoing the following transformation (from GeoPolygonFactory):
    // x1 = x0 cos az + y0 sin az
    // y1 = - x0 sin az + y0 cos az
    // z1 = z0
    // x2 = x1 cos al + z1 sin al
    // y2 = y1
    // z2 = - x1 sin al + z1 cos al
    // So, we reverse the order of the transformations, AND we transform backwards.
    // Transforming backwards means using these identities: sin(-angle) = -sin(angle), cos(-angle) = cos(angle)
    // So:
    // x1 = x0 cos al - z0 sin al
    // y1 = y0
    // z1 = x0 sin al + z0 cos al
    // x2 = x1 cos az - y1 sin az
    // y2 = x1 sin az + y1 cos az
    // z2 = z1
    final double x1 = x * cosLatitude - z * sinLatitude;
    final double y1 = y;
    final double z1 = x * sinLatitude + z * cosLatitude;
    final double x2 = x1 * cosLongitude - y1 * sinLongitude;
    final double y2 = x1 * sinLongitude + y1 * cosLongitude;
    final double z2 = z1;
    // Scale to put the point on the surface
    return pm.createSurfacePoint(x2, y2, z2);
  }
  
  protected static boolean verifyPolygon(final PlanetModel pm, final Polygon polygon, final GeoPolygon outsidePolygon) {
    // Each point in the new poly should be inside the outside poly, and each edge should not intersect the outside poly edge
    final double[] lats = polygon.getPolyLats();
    final double[] lons = polygon.getPolyLons();
    final List<GeoPoint> polyPoints = new ArrayList<>(lats.length-1);
    for (int i = 0; i < lats.length - 1; i++) {
      final GeoPoint newPoint = new GeoPoint(pm, toRadians(lats[i]), toRadians(lons[i]));
      if (!outsidePolygon.isWithin(newPoint)) {
        return false;
      }
      polyPoints.add(newPoint);
    }
    // We don't need to construct the world to find intersections -- just the bordering planes. 
    for (int planeIndex = 0; planeIndex < polyPoints.size(); planeIndex++) {
      final GeoPoint startPoint = polyPoints.get(planeIndex);
      final GeoPoint endPoint = polyPoints.get(legalIndex(planeIndex + 1, polyPoints.size()));
      final GeoPoint beforeStartPoint = polyPoints.get(legalIndex(planeIndex - 1, polyPoints.size()));
      final GeoPoint afterEndPoint = polyPoints.get(legalIndex(planeIndex + 2, polyPoints.size()));
      final SidedPlane beforePlane = new SidedPlane(endPoint, beforeStartPoint, startPoint);
      final SidedPlane afterPlane = new SidedPlane(startPoint, endPoint, afterEndPoint);
      final Plane plane = new Plane(startPoint, endPoint);
      
      // Check for intersections!!
      if (outsidePolygon.intersects(plane, null, beforePlane, afterPlane)) {
        return false;
      }
    }
    return true;
  }
  
  protected static int legalIndex(int index, int size) {
    if (index >= size) {
      index -= size;
    }
    if (index < 0) {
      index += size;
    }
    return index;
  }

  public void testEncodeDecodeCeil() throws Exception {

    // just for testing quantization error
    final double ENCODING_TOLERANCE = Geo3DUtil.DECODE;

    int iters = atLeast(10000);
    for(int iter=0;iter<iters;iter++) {
      GeoPoint point = new GeoPoint(PlanetModel.WGS84, toRadians(GeoTestUtil.nextLatitude()), toRadians(GeoTestUtil.nextLongitude()));
      double xEnc = Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(point.x));
      assertEquals("x=" + point.x + " xEnc=" + xEnc + " diff=" + (point.x - xEnc), point.x, xEnc, ENCODING_TOLERANCE);

      double yEnc = Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(point.y));
      assertEquals("y=" + point.y + " yEnc=" + yEnc + " diff=" + (point.y - yEnc), point.y, yEnc, ENCODING_TOLERANCE);

      double zEnc = Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(point.z));
      assertEquals("z=" + point.z + " zEnc=" + zEnc + " diff=" + (point.z - zEnc), point.z, zEnc, ENCODING_TOLERANCE);
    }

    // check edge/interesting cases explicitly
    double planetMax = PlanetModel.WGS84.getMaximumMagnitude();
    for (double value : new double[] {0.0, -planetMax, planetMax}) {
      assertEquals(value, Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(value)), ENCODING_TOLERANCE);
    }
  }

  /** make sure values always go down: this is important for edge case consistency */
  public void testEncodeDecodeRoundsDown() throws Exception {

    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      final double latBase = GeoTestUtil.nextLatitude();
      final double lonBase = GeoTestUtil.nextLongitude();

      // test above the value
      double lat = latBase;
      double lon = lonBase;
      for (int i = 0; i < 1000; i++) {
        lat = Math.min(90, Math.nextUp(lat));
        lon = Math.min(180, Math.nextUp(lon));
        GeoPoint point = new GeoPoint(PlanetModel.WGS84, toRadians(lat), toRadians(lon));
        GeoPoint pointEnc = new GeoPoint(Geo3DUtil.decodeValueFloor(Geo3DUtil.encodeValue(point.x)),
                                         Geo3DUtil.decodeValueFloor(Geo3DUtil.encodeValue(point.y)),
                                         Geo3DUtil.decodeValueFloor(Geo3DUtil.encodeValue(point.z)));
        assertTrue(pointEnc.x <= point.x);
        assertTrue(pointEnc.y <= point.y);
        assertTrue(pointEnc.z <= point.z);
      }

      // test below the value
      lat = latBase;
      lon = lonBase;
      for (int i = 0; i < 1000; i++) {
        lat = Math.max(-90, Math.nextDown(lat));
        lon = Math.max(-180, Math.nextDown(lon));
        GeoPoint point = new GeoPoint(PlanetModel.WGS84, toRadians(lat), toRadians(lon));
        GeoPoint pointEnc = new GeoPoint(Geo3DUtil.decodeValueFloor(Geo3DUtil.encodeValue(point.x)),
                                         Geo3DUtil.decodeValueFloor(Geo3DUtil.encodeValue(point.y)),
                                         Geo3DUtil.decodeValueFloor(Geo3DUtil.encodeValue(point.z)));
        assertTrue(pointEnc.x <= point.x);
        assertTrue(pointEnc.y <= point.y);
        assertTrue(pointEnc.z <= point.z);
      }
    }
  }

  // poached from TestGeoEncodingUtils.testLatitudeQuantization:

  /**
   * step through some integers, ensuring they decode to their expected double values.
   * double values start at -planetMax and increase by Geo3DUtil.DECODE for each integer.
   * check edge cases within the double range and random doubles within the range too.
   */
  public void testQuantization() throws Exception {
    Random random = random();
    for (int i = 0; i < 10000; i++) {
      int encoded = random.nextInt();
      if (encoded < Geo3DUtil.MIN_ENCODED_VALUE) {
        continue;
      }
      if (encoded > Geo3DUtil.MAX_ENCODED_VALUE) {
        continue;
      }
      double min = encoded * Geo3DUtil.DECODE;
      double decoded = Geo3DUtil.decodeValueFloor(encoded);
      // should exactly equal expected value
      assertEquals(min, decoded, 0.0D);
      // should round-trip
      assertEquals(encoded, Geo3DUtil.encodeValue(decoded));
      // test within the range
      if (encoded != Integer.MAX_VALUE) {
        // this is the next representable value
        // all double values between [min .. max) should encode to the current integer
        double max = min + Geo3DUtil.DECODE;
        assertEquals(max, Geo3DUtil.decodeValueFloor(encoded+1), 0.0D);
        assertEquals(encoded+1, Geo3DUtil.encodeValue(max));

        // first and last doubles in range that will be quantized
        double minEdge = Math.nextUp(min);
        double maxEdge = Math.nextDown(max);
        assertEquals(encoded, Geo3DUtil.encodeValue(minEdge));
        assertEquals(encoded, Geo3DUtil.encodeValue(maxEdge));

        // check random values within the double range
        long minBits = NumericUtils.doubleToSortableLong(minEdge);
        long maxBits = NumericUtils.doubleToSortableLong(maxEdge);
        for (int j = 0; j < 100; j++) {
          double value = NumericUtils.sortableLongToDouble(TestUtil.nextLong(random, minBits, maxBits));
          // round down
          assertEquals(encoded,   Geo3DUtil.encodeValue(value));
        }
      }
    }
  }

  public void testEncodeDecodeIsStable() throws Exception {

    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      double lat = GeoTestUtil.nextLatitude();
      double lon = GeoTestUtil.nextLongitude();

      GeoPoint point = new GeoPoint(PlanetModel.WGS84, toRadians(lat), toRadians(lon));

      // encode point
      GeoPoint pointEnc = new GeoPoint(Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(point.x)),
                                       Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(point.y)),
                                       Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(point.z)));

      // encode it again (double encode)
      GeoPoint pointEnc2 = new GeoPoint(Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(pointEnc.x)),
                                        Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(pointEnc.y)),
                                        Geo3DUtil.decodeValue(Geo3DUtil.encodeValue(pointEnc.z)));
      //System.out.println("TEST " + iter + ":\n  point    =" + point + "\n  pointEnc =" + pointEnc + "\n  pointEnc2=" + pointEnc2);
    
      assertEquals(pointEnc.x, pointEnc2.x, 0.0);
      assertEquals(pointEnc.y, pointEnc2.y, 0.0);
      assertEquals(pointEnc.z, pointEnc2.z, 0.0);
    }
  }

  /** Clips the incoming value to the allowed min/max range before encoding, instead of throwing an exception. */
  private static int encodeValueLenient(double x) {
    double planetMax = PlanetModel.WGS84.getMaximumMagnitude();
    if (x > planetMax) {
      x = planetMax;
    } else if (x < -planetMax) {
      x = -planetMax;
    }
    return Geo3DUtil.encodeValue(x);
  }

  private static class ExplainingVisitor implements IntersectVisitor {

    final GeoShape shape;
    final GeoPoint targetDocPoint;
    final GeoPoint scaledDocPoint;
    final IntersectVisitor in;
    final List<Cell> stack = new ArrayList<>();
    private List<Cell> stackToTargetDoc;
    final int targetDocID;
    final int numDims;
    final int bytesPerDim;
    private int targetStackUpto;
    final StringBuilder b;

    // In the first phase, we always return CROSSES to do a full scan of the BKD tree to see which leaf block the document lives in
    boolean firstPhase = true;

    public ExplainingVisitor(GeoShape shape, GeoPoint targetDocPoint, GeoPoint scaledDocPoint, IntersectVisitor in, int targetDocID, int numDims, int bytesPerDim, StringBuilder b) {
      this.shape = shape;
      this.targetDocPoint = targetDocPoint;
      this.scaledDocPoint = scaledDocPoint;
      this.in = in;
      this.targetDocID = targetDocID;
      this.numDims = numDims;
      this.bytesPerDim = bytesPerDim;
      this.b = b;
    }

    public void startSecondPhase() {
      if (firstPhase == false) {
        throw new IllegalStateException("already started second phase");
      }
      if (stackToTargetDoc == null) {
        b.append("target docID=" + targetDocID + " was never seen in points!\n");
      }
      firstPhase = false;
      stack.clear();
    }

    @Override
    public void visit(int docID) throws IOException {
      assert firstPhase == false;
      if (docID == targetDocID) {
        b.append("leaf visit docID=" + docID + "\n");
      }
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      if (firstPhase) {
        if (docID == targetDocID) {
          assert stackToTargetDoc == null;
          stackToTargetDoc = new ArrayList<>(stack);
          b.append("  full BKD path to target doc:\n");
          for(Cell cell : stack) {
            b.append("    " + cell + "\n");
          }
        }
      } else {
        if (docID == targetDocID) {
          double x = Geo3DPoint.decodeDimension(packedValue, 0);
          double y = Geo3DPoint.decodeDimension(packedValue, Integer.BYTES);
          double z = Geo3DPoint.decodeDimension(packedValue, 2 * Integer.BYTES);
          b.append("leaf visit docID=" + docID + " x=" + x + " y=" + y + " z=" + z + "\n");
          in.visit(docID, packedValue);
        }
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      Cell cell = new Cell(minPackedValue, maxPackedValue);
      //System.out.println("compare: " + cell);

      // TODO: this is a bit hacky, having to reverse-engineer where we are in the BKD tree's recursion ... but it's the lesser evil vs e.g.
      // polluting this visitor API, or implementing this "under the hood" in BKDReader instead?
      if (firstPhase) {

        // Pop stack:
        while (stack.size() > 0 && stack.get(stack.size()-1).contains(cell) == false) {
          stack.remove(stack.size()-1);
          //System.out.println("  pop");
        }

        // Push stack:
        stack.add(cell);
        //System.out.println("  push");

        return Relation.CELL_CROSSES_QUERY;
      } else {
        Relation result = in.compare(minPackedValue, maxPackedValue);
        if (targetStackUpto < stackToTargetDoc.size() && cell.equals(stackToTargetDoc.get(targetStackUpto))) {
          b.append("  on cell " + stackToTargetDoc.get(targetStackUpto) + ", wrapped visitor returned " + result + "\n");
          targetStackUpto++;
        }
        return result;
      }
    }

    private class Cell {
      private final byte[] minPackedValue;
      private final byte[] maxPackedValue;

      public Cell(byte[] minPackedValue, byte[] maxPackedValue) {
        this.minPackedValue = minPackedValue.clone();
        this.maxPackedValue = maxPackedValue.clone();
      }

      /** Returns true if this cell fully contains the other one */
      public boolean contains(Cell other) {
        for(int dim=0;dim<numDims;dim++) {
          int offset = bytesPerDim * dim;
          // other.min < this.min?
          if (StringHelper.compare(bytesPerDim, other.minPackedValue, offset, minPackedValue, offset) < 0) {
            return false;
          }
          // other.max < this.max?
          if (StringHelper.compare(bytesPerDim, other.maxPackedValue, offset, maxPackedValue, offset) > 0) {
            return false;
          }
        }

        return true;
      }

      @Override
      public String toString() {
        double xMin = Geo3DUtil.decodeValueFloor(NumericUtils.sortableBytesToInt(minPackedValue, 0));
        double xMax = Geo3DUtil.decodeValueCeil(NumericUtils.sortableBytesToInt(maxPackedValue, 0));
        double yMin = Geo3DUtil.decodeValueFloor(NumericUtils.sortableBytesToInt(minPackedValue, 1 * Integer.BYTES));
        double yMax = Geo3DUtil.decodeValueCeil(NumericUtils.sortableBytesToInt(maxPackedValue, 1 * Integer.BYTES));
        double zMin = Geo3DUtil.decodeValueFloor(NumericUtils.sortableBytesToInt(minPackedValue, 2 * Integer.BYTES));
        double zMax = Geo3DUtil.decodeValueCeil(NumericUtils.sortableBytesToInt(maxPackedValue, 2 * Integer.BYTES));
        final XYZSolid xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, xMin, xMax, yMin, yMax, zMin, zMax);
        final int relationship = xyzSolid.getRelationship(shape);
        final boolean pointWithinCell = xyzSolid.isWithin(targetDocPoint);
        final boolean scaledWithinCell = xyzSolid.isWithin(scaledDocPoint);

        final String relationshipString;
        switch (relationship) {
        case GeoArea.CONTAINS:
          relationshipString = "CONTAINS";
          break;
        case GeoArea.WITHIN:
          relationshipString = "WITHIN";
          break;
        case GeoArea.OVERLAPS:
          relationshipString = "OVERLAPS";
          break;
        case GeoArea.DISJOINT:
          relationshipString = "DISJOINT";
          break;
        default:
          relationshipString = "UNKNOWN";
          break;
        }
        return "Cell(x=" + xMin + " TO " + xMax + " y=" + yMin + " TO " + yMax + " z=" + zMin + " TO " + zMax +
          "); Shape relationship = "+relationshipString+
          "; Quantized point within cell = "+pointWithinCell+
          "; Unquantized point within cell = "+scaledWithinCell;
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof Cell == false) {
          return false;
        }

        Cell otherCell = (Cell) other;
        return Arrays.equals(minPackedValue, otherCell.minPackedValue) && Arrays.equals(maxPackedValue, otherCell.maxPackedValue);
      }

      @Override
      public int hashCode() {
        return Arrays.hashCode(minPackedValue) + Arrays.hashCode(maxPackedValue);
      }
    }
  }

  public static String explain(String fieldName, GeoShape shape, GeoPoint targetDocPoint, GeoPoint scaledDocPoint, IndexReader reader, int docID) throws Exception {

    final XYZBounds bounds = new XYZBounds();
    shape.getBounds(bounds);
    
    // First find the leaf reader that owns this doc:
    int subIndex = ReaderUtil.subIndex(docID, reader.leaves());
    LeafReader leafReader = reader.leaves().get(subIndex).reader();

    StringBuilder b = new StringBuilder();
    b.append("target is in leaf " + leafReader + " of full reader " + reader + "\n");

    DocIdSetBuilder hits = new DocIdSetBuilder(leafReader.maxDoc());
    ExplainingVisitor visitor = new ExplainingVisitor(shape, targetDocPoint, scaledDocPoint,
      new PointInShapeIntersectVisitor(hits, shape, bounds),
      docID - reader.leaves().get(subIndex).docBase, 3, Integer.BYTES, b);

    // Do first phase, where we just figure out the "path" that leads to the target docID:
    leafReader.getPointValues().intersect(fieldName, visitor);

    // Do second phase, where we we see how the wrapped visitor responded along that path:
    visitor.startSecondPhase();
    leafReader.getPointValues().intersect(fieldName, visitor);

    return b.toString();
  }
}
