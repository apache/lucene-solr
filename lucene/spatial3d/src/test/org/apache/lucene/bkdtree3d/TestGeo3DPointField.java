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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene53.Lucene53Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.geo3d.GeoBBoxFactory;
import org.apache.lucene.geo3d.GeoCircle;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.geo3d.PlanetModel;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.decodeValue;
import static org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat.encodeValue;

public class TestGeo3DPointField extends LuceneTestCase {

  private static boolean smallBBox;

  @BeforeClass
  public static void beforeClass() {
    smallBBox = random().nextBoolean();
    if (VERBOSE) {
      System.out.println("TEST: smallBBox=" + smallBBox);
    }
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.alwaysDocValuesFormat(new Geo3DDocValuesFormat(maxPointsInLeaf, maxPointsSortInHeap)));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new Geo3DPointField("field", PlanetModel.WGS84, toRadians(50.7345267), toRadians(-97.5303555)));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    // We can't wrap with "exotic" readers because the query must see the BKD3DDVFormat:
    IndexSearcher s = newSearcher(r, false);
    assertEquals(1, s.search(new PointInGeo3DShapeQuery(PlanetModel.WGS84,
                                                        "field",
                                                        new GeoCircle(PlanetModel.WGS84, toRadians(50), toRadians(-97), Math.PI/180.)), 1).totalHits);
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
    return (random().nextDouble()*2.0022) - 1.0011;
    //return (random().nextDouble()*.002) - .001;
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

  public void testBKDRandom() throws Exception {
    List<Point> points = new ArrayList<>();
    int numPoints = atLeast(10000);
    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048); 

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

        // We must quantize exactly as BKD tree does else we'll get false failures
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
      System.out.println("TEST: numPoints=" + numPoints);
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
          System.out.println("  doc=" + docID + " is missing");
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
            System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat as doc=" + oldDocID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[docID] = toRadians(randomLat());
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lon as doc=" + oldDocID + ")");
          }
        } else {
          assert x == 2;
          // Fully identical point:
          lats[docID] = lats[oldDocID];
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat/lon as doc=" + oldDocID + ")");
          }
        }
      } else {
        lats[docID] = toRadians(randomLat());
        lons[docID] = toRadians(randomLon());
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID]);
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

  private static void verify(double[] lats, double[] lons) throws Exception {
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    IndexWriterConfig iwc = newIndexWriterConfig();

    PlanetModel planetModel;
    if (random().nextBoolean()) {
      planetModel = PlanetModel.WGS84;
    } else {
      planetModel = PlanetModel.SPHERE;
    }

    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < lats.length/100) {
      iwc.setMaxBufferedDocs(lats.length/100);
    }
    final DocValuesFormat dvFormat = new Geo3DDocValuesFormat(maxPointsInLeaf, maxPointsSortInHeap);
    Codec codec = new Lucene53Codec() {
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
    Set<Integer> deleted = new HashSet<>();
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
          System.out.println("  delete id=" + idToDelete);
        }
      }
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    final IndexReader r = DirectoryReader.open(w, true);
    w.close();

    // We can't wrap with "exotic" readers because the geo3d query must see the Geo3DDVFormat:
    IndexSearcher s = newSearcher(r, false);

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

              // nocommit randomize other shapes?

              GeoShape shape;
              if (random().nextBoolean()) {
                double lat = toRadians(randomLat());
                double lon = toRadians(randomLon());

                double angle;
                if (smallBBox) {
                  angle = random().nextDouble() * Math.PI/360.0;
                } else {
                  angle = random().nextDouble() * Math.PI/2.0;
                }

                if (VERBOSE) {
                  System.out.println("\nTEST: iter=" + iter + " shape=GeoCircle lat=" + lat + " lon=" + lon + " angle=" + angle);
                }

                shape = new GeoCircle(planetModel, lat, lon, angle);

              } else {
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

                if (VERBOSE) {
                  System.out.println("\nTEST: iter=" + iter + " shape=GeoBBox lat0=" + lat0 + " lat1=" + lat1 + " lon0=" + lon0 + " lon1=" + lon1);
                }

                shape = GeoBBoxFactory.makeGeoBBox(planetModel, lat1, lat0, lon0, lon1);
              }

              Query query = new PointInGeo3DShapeQuery(planetModel, "point", shape);

              if (VERBOSE) {
                System.out.println("  using query: " + query);
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
                System.out.println("  hitCount: " + hits.cardinality());
              }
      
              for(int docID=0;docID<r.maxDoc();docID++) {
                int id = (int) docIDToID.get(docID);
                if (Double.isNaN(lats[id]) == false) {

                  // Accurate point:
                  GeoPoint point1 = new GeoPoint(planetModel, lats[id], lons[id]);

                  // Quantized point (32 bits per dim):
                  GeoPoint point2 = new GeoPoint(decodeValue(encodeValue(point1.x)),
                                                 decodeValue(encodeValue(point1.y)),
                                                 decodeValue(encodeValue(point1.z)));

                  boolean expected = deleted.contains(id) == false && shape.isWithin(point2);
                  if (hits.get(docID) != expected) {
                    fail(Thread.currentThread().getName() + ": iter=" + iter + " id=" + id + " docID=" + docID + " lat=" + lats[id] + " lon=" + lons[id] + " expected " + expected + " but got: " + hits.get(docID) + " deleted?=" + deleted.contains(id) + "\n  point1=" + point1 + "\n  point2=" + point2 + "\n  query=" + query);
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

