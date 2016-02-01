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
package org.apache.lucene.spatial.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

import static org.apache.lucene.spatial.util.GeoDistanceUtils.DISTANCE_PCT_ERR;

/**
 * Tests class for methods in GeoUtils
 *
 * @lucene.experimental
 */
public class TestGeoUtils extends LuceneTestCase {

  private static final double LON_SCALE = (0x1L<<GeoEncodingUtils.BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<GeoEncodingUtils.BITS)/180.0D;

  // Global bounding box we will "cover" in the random test; we have to make this "smallish" else the queries take very long:
  private static double originLat;
  private static double originLon;
  //  private static double range;
  private static double lonRange;
  private static double latRange;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Between 1.0 and 3.0:
    lonRange = 2 * (random().nextDouble() + 0.5);
    latRange = 2 * (random().nextDouble() + 0.5);

    originLon = GeoUtils.MIN_LON_INCL + lonRange + (GeoUtils.MAX_LON_INCL - GeoUtils.MIN_LON_INCL - 2 * lonRange) * random().nextDouble();
    originLon = GeoUtils.normalizeLon(originLon);
    originLat = GeoUtils.MIN_LAT_INCL + latRange + (GeoUtils.MAX_LAT_INCL - GeoUtils.MIN_LAT_INCL - 2 * latRange) * random().nextDouble();
    originLat = GeoUtils.normalizeLat(originLat);

    if (VERBOSE) {
      System.out.println("TEST: originLon=" + originLon + " lonRange= " + lonRange + " originLat=" + originLat + " latRange=" + latRange);
    }
  }

  public void testGeoHash() {
    int numPoints = atLeast(100);
    String randomGeoHashString;
    String mortonGeoHash;
    long mortonLongFromGHLong, geoHashLong, mortonLongFromGHString;
    int randomLevel;
    for (int i = 0; i < numPoints; ++i) {
      // random point
      double lat = randomLat(false);
      double lon = randomLon(false);

      // compute geohash straight from lat/lon and from morton encoded value to ensure they're the same
      randomGeoHashString = GeoHashUtils.stringEncode(lon, lat, randomLevel = random().nextInt(12 - 1) + 1);
      mortonGeoHash = GeoHashUtils.stringEncodeFromMortonLong(GeoEncodingUtils.mortonHash(lon, lat), randomLevel);
      assertEquals(randomGeoHashString, mortonGeoHash);

      // v&v conversion from lat/lon or geohashstring to geohash long and back to geohash string
      geoHashLong = (random().nextBoolean()) ? GeoHashUtils.longEncode(lon, lat, randomLevel) : GeoHashUtils.longEncode(randomGeoHashString);
      assertEquals(randomGeoHashString, GeoHashUtils.stringEncode(geoHashLong));

      // v&v conversion from geohash long to morton long
      mortonLongFromGHString = GeoHashUtils.mortonEncode(randomGeoHashString);
      mortonLongFromGHLong = GeoHashUtils.mortonEncode(geoHashLong);
      assertEquals(mortonLongFromGHLong, mortonLongFromGHString);

      // v&v lat/lon from geohash string and geohash long
      assertEquals(GeoEncodingUtils.mortonUnhashLat(mortonLongFromGHString), GeoEncodingUtils.mortonUnhashLat(mortonLongFromGHLong), 0);
      assertEquals(GeoEncodingUtils.mortonUnhashLon(mortonLongFromGHString), GeoEncodingUtils.mortonUnhashLon(mortonLongFromGHLong), 0);
    }
  }

  /**
   * Pass condition: lat=42.6, lng=-5.6 should be encoded as "ezs42e44yx96",
   * lat=57.64911 lng=10.40744 should be encoded as "u4pruydqqvj8"
   */
  public void testEncode() {
    String hash = GeoHashUtils.stringEncode(-5.6, 42.6, 12);
    assertEquals("ezs42e44yx96", hash);

    hash = GeoHashUtils.stringEncode(10.40744, 57.64911, 12);
    assertEquals("u4pruydqqvj8", hash);
  }

  /**
   * Pass condition: lat=52.3738007, lng=4.8909347 should be encoded and then
   * decoded within 0.00001 of the original value
   */
  public void testDecodePreciseLongitudeLatitude() {
    final String geohash = GeoHashUtils.stringEncode(4.8909347, 52.3738007);
    final long hash = GeoHashUtils.mortonEncode(geohash);

    assertEquals(52.3738007, GeoEncodingUtils.mortonUnhashLat(hash), 0.00001D);
    assertEquals(4.8909347, GeoEncodingUtils.mortonUnhashLon(hash), 0.00001D);
  }

  /**
   * Pass condition: lat=84.6, lng=10.5 should be encoded and then decoded
   * within 0.00001 of the original value
   */
  public void testDecodeImpreciseLongitudeLatitude() {
    final String geohash = GeoHashUtils.stringEncode(10.5, 84.6);

    final long hash = GeoHashUtils.mortonEncode(geohash);

    assertEquals(84.6, GeoEncodingUtils.mortonUnhashLat(hash), 0.00001D);
    assertEquals(10.5, GeoEncodingUtils.mortonUnhashLon(hash), 0.00001D);
  }

  public void testDecodeEncode() {
    final String geoHash = "u173zq37x014";
    assertEquals(geoHash, GeoHashUtils.stringEncode(4.8909347, 52.3738007));
    final long mortonHash = GeoHashUtils.mortonEncode(geoHash);
    final double lon = GeoEncodingUtils.mortonUnhashLon(mortonHash);
    final double lat = GeoEncodingUtils.mortonUnhashLat(mortonHash);
    assertEquals(52.37380061d, GeoEncodingUtils.mortonUnhashLat(mortonHash), 0.000001d);
    assertEquals(4.8909343d, GeoEncodingUtils.mortonUnhashLon(mortonHash), 0.000001d);

    assertEquals(geoHash, GeoHashUtils.stringEncode(lon, lat));
  }

  public void testNeighbors() {
    String geohash = "gcpv";
    List<String> expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("gcpw");
    expectedNeighbors.add("gcpy");
    expectedNeighbors.add("u10n");
    expectedNeighbors.add("gcpt");
    expectedNeighbors.add("u10j");
    expectedNeighbors.add("gcps");
    expectedNeighbors.add("gcpu");
    expectedNeighbors.add("u10h");
    Collection<? super String> neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors );
    assertEquals(expectedNeighbors, neighbors);

    // Border odd geohash
    geohash = "u09x";
    expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("u0c2");
    expectedNeighbors.add("u0c8");
    expectedNeighbors.add("u0cb");
    expectedNeighbors.add("u09r");
    expectedNeighbors.add("u09z");
    expectedNeighbors.add("u09q");
    expectedNeighbors.add("u09w");
    expectedNeighbors.add("u09y");
    neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors);
    assertEquals(expectedNeighbors, neighbors);

    // Border even geohash
    geohash = "u09tv";
    expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("u09wh");
    expectedNeighbors.add("u09wj");
    expectedNeighbors.add("u09wn");
    expectedNeighbors.add("u09tu");
    expectedNeighbors.add("u09ty");
    expectedNeighbors.add("u09ts");
    expectedNeighbors.add("u09tt");
    expectedNeighbors.add("u09tw");
    neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors );
    assertEquals(expectedNeighbors, neighbors);

    // Border even and odd geohash
    geohash = "ezzzz";
    expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("gbpbn");
    expectedNeighbors.add("gbpbp");
    expectedNeighbors.add("u0000");
    expectedNeighbors.add("ezzzy");
    expectedNeighbors.add("spbpb");
    expectedNeighbors.add("ezzzw");
    expectedNeighbors.add("ezzzx");
    expectedNeighbors.add("spbp8");
    neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors );
    assertEquals(expectedNeighbors, neighbors);
  }

  public void testClosestPointOnBBox() {
    double[] result = new double[2];
    GeoDistanceUtils.closestPointOnBBox(20, 30, 40, 50, 70, 70, result);
    assertEquals(40.0, result[0], 0.0);
    assertEquals(50.0, result[1], 0.0);

    GeoDistanceUtils.closestPointOnBBox(-20, -20, 0, 0, 70, 70, result);
    assertEquals(0.0, result[0], 0.0);
    assertEquals(0.0, result[1], 0.0);
  }

  private static class Cell {
    static int nextCellID;

    final Cell parent;
    final int cellID;
    final double minLon, maxLon;
    final double minLat, maxLat;
    final int splitCount;

    public Cell(Cell parent,
                double minLon, double minLat,
                double maxLon, double maxLat,
                int splitCount) {
      assert maxLon >= minLon;
      assert maxLat >= minLat;
      this.parent = parent;
      this.minLon = minLon;
      this.minLat = minLat;
      this.maxLon = maxLon;
      this.maxLat = maxLat;
      this.cellID = nextCellID++;
      this.splitCount = splitCount;
    }

    /** Returns true if the quantized point lies within this cell, inclusive on all bounds. */
    public boolean contains(double lon, double lat) {
      return lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat;
    }

    @Override
    public String toString() {
      return "cell=" + cellID + (parent == null ? "" : " parentCellID=" + parent.cellID) + " lon: " + minLon + " TO " + maxLon + ", lat: " + minLat + " TO " + maxLat + ", splits: " + splitCount;
    }
  }

  public long scaleLon(final double val) {
    return (long) ((val-GeoUtils.MIN_LON_INCL) * LON_SCALE);
  }

  public long scaleLat(final double val) {
    return (long) ((val-GeoUtils.MIN_LAT_INCL) * LAT_SCALE);
  }

  public double unscaleLon(final long val) {
    return (val / LON_SCALE) + GeoUtils.MIN_LON_INCL;
  }

  public double unscaleLat(final long val) {
    return (val / LAT_SCALE) + GeoUtils.MIN_LAT_INCL;
  }

  public double randomLat(boolean small) {
    double result;
    if (small) {
      result = GeoUtils.normalizeLat(originLat + latRange * (random().nextDouble() - 0.5));
    } else {
      result = -90 + 180.0 * random().nextDouble();
    }
    return result;
  }

  public double randomLon(boolean small) {
    double result;
    if (small) {
      result = GeoUtils.normalizeLon(originLon + lonRange * (random().nextDouble() - 0.5));
    } else {
      result = -180 + 360.0 * random().nextDouble();
    }
    return result;
  }

  private void findMatches(Set<Integer> hits, PrintWriter log, Cell root,
                           double centerLon, double centerLat, double radiusMeters,
                           double[] docLons, double[] docLats) {

    if (VERBOSE) {
      log.println("  root cell: " + root);
    }

    List<Cell> queue = new ArrayList<>();
    queue.add(root);

    int recurseDepth = RandomInts.randomIntBetween(random(), 5, 15);

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
        for(int docID=0;docID<docLons.length;docID++) {
          if (cell.contains(docLons[docID], docLats[docID])) {
            double distanceMeters = GeoDistanceUtils.haversin(centerLat, centerLon, docLats[docID], docLons[docID]);
            if (distanceMeters <= radiusMeters) {
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

        if (GeoRelationUtils.rectWithinCircle(cell.minLon, cell.minLat, cell.maxLon, cell.maxLat, centerLon, centerLat, radiusMeters)) {
          // Query circle fully contains this cell, just addAll:
          if (VERBOSE) {
            log.println("    circle fully contains cell: now addAll");
          }
          for(int docID=0;docID<docLons.length;docID++) {
            if (cell.contains(docLons[docID], docLats[docID])) {
              if (VERBOSE) {
                log.println("    addAll doc=" + docID);
              }
              hits.add(docID);
            }
          }
          continue;
        } else if (GeoRelationUtils.rectWithin(root.minLon, root.minLat, root.maxLon, root.maxLat,
            cell.minLon, cell.minLat, cell.maxLon, cell.maxLat)) {
          // Fall through below to "recurse"
          if (VERBOSE) {
            log.println("    cell fully contains circle: keep splitting");
          }
        } else if (GeoRelationUtils.rectCrossesCircle(cell.minLon, cell.minLat, cell.maxLon, cell.maxLat,
            centerLon, centerLat, radiusMeters)) {
          // Fall through below to "recurse"
          if (VERBOSE) {
            log.println("    cell overlaps circle: keep splitting");
          }
        } else {
          if (VERBOSE) {
            log.println("    no overlap: drop this cell");
            for(int docID=0;docID<docLons.length;docID++) {
              if (cell.contains(docLons[docID], docLats[docID])) {
                if (VERBOSE) {
                  log.println("    skip doc=" + docID);
                }
              }
            }
          }
          continue;
        }

        // Randomly split:
        if (random().nextBoolean()) {

          // Split on lon:
          double splitValue = cell.minLon + (cell.maxLon - cell.minLon) * random().nextDouble();
          if (VERBOSE) {
            log.println("    now split on lon=" + splitValue);
          }
          Cell cell1 = new Cell(cell,
              cell.minLon, cell.minLat,
              splitValue, cell.maxLat,
              cell.splitCount+1);
          Cell cell2 = new Cell(cell,
              splitValue, cell.minLat,
              cell.maxLon, cell.maxLat,
              cell.splitCount+1);
          if (VERBOSE) {
            log.println("    split cell1: " + cell1);
            log.println("    split cell2: " + cell2);
          }
          queue.add(cell1);
          queue.add(cell2);
        } else {

          // Split on lat:
          double splitValue = cell.minLat + (cell.maxLat - cell.minLat) * random().nextDouble();
          if (VERBOSE) {
            log.println("    now split on lat=" + splitValue);
          }
          Cell cell1 = new Cell(cell,
              cell.minLon, cell.minLat,
              cell.maxLon, splitValue,
              cell.splitCount+1);
          Cell cell2 = new Cell(cell,
              cell.minLon, splitValue,
              cell.maxLon, cell.maxLat,
              cell.splitCount+1);
          if (VERBOSE) {
            log.println("    split cells:\n      " + cell1 + "\n      " + cell2);
          }
          queue.add(cell1);
          queue.add(cell2);
        }
      }
    }
  }

  /** Tests consistency of GeoEncodingUtils.rectWithinCircle, .rectCrossesCircle, .rectWithin and SloppyMath.haversine distance check */
  public void testGeoRelations() throws Exception {

    int numDocs = atLeast(1000);

    boolean useSmallRanges = random().nextBoolean();

    if (VERBOSE) {
      System.out.println("TEST: " + numDocs + " docs useSmallRanges=" + useSmallRanges);
    }

    double[] docLons = new double[numDocs];
    double[] docLats = new double[numDocs];
    for(int docID=0;docID<numDocs;docID++) {
      docLons[docID] = randomLon(useSmallRanges);
      docLats[docID] = randomLat(useSmallRanges);
      if (VERBOSE) {
        System.out.println("  doc=" + docID + ": lon=" + docLons[docID] + " lat=" + docLats[docID]);
      }
    }

    int iters = atLeast(10);

    iters = atLeast(50);

    for(int iter=0;iter<iters;iter++) {

      Cell.nextCellID = 0;

      double centerLon = randomLon(useSmallRanges);
      double centerLat = randomLat(useSmallRanges);

      // So the circle covers at most 50% of the earth's surface:

      double radiusMeters;

      // TODO: large exotic rectangles created by BKD may be inaccurate up to 2 times DISTANCE_PCT_ERR.
      // restricting size until LUCENE-6994 can be addressed
      if (true || useSmallRanges) {
        // Approx 3 degrees lon at the equator:
        radiusMeters = random().nextDouble() * 333000;
      } else {
        radiusMeters = random().nextDouble() * GeoProjectionUtils.SEMIMAJOR_AXIS * Math.PI / 2.0;
      }

      StringWriter sw = new StringWriter();
      PrintWriter log = new PrintWriter(sw, true);

      if (VERBOSE) {
        log.println("\nTEST: iter=" + iter + " radiusMeters=" + radiusMeters + " centerLon=" + centerLon + " centerLat=" + centerLat);
      }

      GeoRect bbox = GeoUtils.circleToBBox(centerLon, centerLat, radiusMeters);

      Set<Integer> hits = new HashSet<>();

      if (bbox.maxLon < bbox.minLon) {
        // Crosses dateline
        log.println("  circle crosses dateline; first left query");
        double unwrappedLon = centerLon;
        if (unwrappedLon > bbox.maxLon) {
          // unwrap left
          unwrappedLon += -360.0D;
        }
        findMatches(hits, log,
            new Cell(null,
                -180, bbox.minLat,
                bbox.maxLon, bbox.maxLat,
                0),
            unwrappedLon, centerLat, radiusMeters, docLons, docLats);
        log.println("  circle crosses dateline; now right query");
        if (unwrappedLon < bbox.maxLon) {
          // unwrap right
          unwrappedLon += 360.0D;
        }
        findMatches(hits, log,
            new Cell(null,
                bbox.minLon, bbox.minLat,
                180, bbox.maxLat,
                0),
            unwrappedLon, centerLat, radiusMeters, docLons, docLats);
      } else {
        // Start with the root cell that fully contains the shape:
        findMatches(hits, log,
            new Cell(null,
                bbox.minLon, bbox.minLat,
                bbox.maxLon, bbox.maxLat,
                0),
            centerLon, centerLat, radiusMeters,
            docLons, docLats);
      }

      if (VERBOSE) {
        log.println("  " + hits.size() + " hits");
      }

      int failCount = 0;

      // Done matching, now verify:
      for(int docID=0;docID<numDocs;docID++) {
        double distanceMeters = GeoDistanceUtils.haversin(centerLat, centerLon, docLats[docID], docLons[docID]);
        final Boolean expected;
        final double percentError = Math.abs(distanceMeters - radiusMeters) / distanceMeters;
        if (percentError <= DISTANCE_PCT_ERR) {
          expected = null;
        } else {
          expected = distanceMeters <= radiusMeters;
        }

        boolean actual = hits.contains(docID);
        if (expected != null && actual != expected) {
          if (actual) {
            log.println("doc=" + docID + " matched but should not with distance error " + percentError + " on iteration " + iter);
          } else {
            log.println("doc=" + docID + " did not match but should with distance error " + percentError + " on iteration " + iter);
          }
          log.println("  lon=" + docLons[docID] + " lat=" + docLats[docID] + " distanceMeters=" + distanceMeters + " vs radiusMeters=" + radiusMeters);
          failCount++;
        }
      }

      if (failCount != 0) {
        System.out.print(sw.toString());
        fail(failCount + " incorrect hits (see above)");
      }
    }
  }

  /**
   * Tests stability of {@link GeoEncodingUtils#geoCodedToPrefixCoded}
   */
  public void testGeoPrefixCoding() throws Exception {
    int numIters = atLeast(1000);
    long hash;
    long decodedHash;
    BytesRefBuilder brb = new BytesRefBuilder();
    while (numIters-- >= 0) {
      hash = GeoEncodingUtils.mortonHash(randomLon(false), randomLat(false));
      for (int i=32; i<64; ++i) {
        GeoEncodingUtils.geoCodedToPrefixCoded(hash, i, brb);
        decodedHash = GeoEncodingUtils.prefixCodedToGeoCoded(brb.get());
        assertEquals((hash >>> i) << i, decodedHash);
      }
    }
  }
}
