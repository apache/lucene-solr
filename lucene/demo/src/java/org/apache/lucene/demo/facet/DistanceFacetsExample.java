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
package org.apache.lucene.demo.facet;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.SloppyMath;

/** Shows simple usage of dynamic range faceting, using the
 *  expressions module to calculate distance. */
public class DistanceFacetsExample implements Closeable {

  final DoubleRange ONE_KM = new DoubleRange("< 1 km", 0.0, true, 1.0, false);
  final DoubleRange TWO_KM = new DoubleRange("< 2 km", 0.0, true, 2.0, false);
  final DoubleRange FIVE_KM = new DoubleRange("< 5 km", 0.0, true, 5.0, false);
  final DoubleRange TEN_KM = new DoubleRange("< 10 km", 0.0, true, 10.0, false);

  private final Directory indexDir = new RAMDirectory();
  private IndexSearcher searcher;
  private final FacetsConfig config = new FacetsConfig();

  /** The "home" latitude. */
  public final static double ORIGIN_LATITUDE = 40.7143528;

  /** The "home" longitude. */
  public final static double ORIGIN_LONGITUDE = -74.0059731;

  /** Mean radius of the Earth in KM
   *
   * NOTE: this is approximate, because the earth is a bit
   * wider at the equator than the poles.  See
   * http://en.wikipedia.org/wiki/Earth_radius */
  // see http://earth-info.nga.mil/GandG/publications/tr8350.2/wgs84fin.pdf
  public final static double EARTH_RADIUS_KM = 6_371.0087714;

  /** Empty constructor */
  public DistanceFacetsExample() {}
  
  /** Build the example index. */
  public void index() throws IOException {
    IndexWriter writer = new IndexWriter(indexDir, new IndexWriterConfig(
        new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    // TODO: we could index in radians instead ... saves all the conversions in getBoundingBoxFilter

    // Add documents with latitude/longitude location:
    // we index these both as DoublePoints (for bounding box/ranges) and as NumericDocValuesFields (for scoring)
    Document doc = new Document();
    doc.add(new DoublePoint("latitude", 40.759011));
    doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.759011)));
    doc.add(new DoublePoint("longitude", -73.9844722));
    doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits(-73.9844722)));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoublePoint("latitude", 40.718266));
    doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.718266)));
    doc.add(new DoublePoint("longitude", -74.007819));
    doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits(-74.007819)));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoublePoint("latitude", 40.7051157));
    doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.7051157)));
    doc.add(new DoublePoint("longitude", -74.0088305));
    doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits(-74.0088305)));
    writer.addDocument(doc);

    // Open near-real-time searcher
    searcher = new IndexSearcher(DirectoryReader.open(writer));
    writer.close();
  }

  private DoubleValuesSource getDistanceValueSource() {
    Expression distance;
    try {
      distance = JavascriptCompiler.compile(
                  "haversin(" + ORIGIN_LATITUDE + "," + ORIGIN_LONGITUDE + ",latitude,longitude)");
    } catch (ParseException pe) {
      // Should not happen
      throw new RuntimeException(pe);
    }
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("latitude", SortField.Type.DOUBLE));
    bindings.add(new SortField("longitude", SortField.Type.DOUBLE));

    return distance.getDoubleValuesSource(bindings);
  }

  /** Given a latitude and longitude (in degrees) and the
   *  maximum great circle (surface of the earth) distance,
   *  returns a simple Filter bounding box to "fast match"
   *  candidates. */
  public static Query getBoundingBoxQuery(double originLat, double originLng, double maxDistanceKM) {

    // Basic bounding box geo math from
    // http://JanMatuschek.de/LatitudeLongitudeBoundingCoordinates,
    // licensed under creative commons 3.0:
    // http://creativecommons.org/licenses/by/3.0

    // TODO: maybe switch to recursive prefix tree instead
    // (in lucene/spatial)?  It should be more efficient
    // since it's a 2D trie...

    // Degrees -> Radians:
    double originLatRadians = SloppyMath.toRadians(originLat);
    double originLngRadians = SloppyMath.toRadians(originLng);

    double angle = maxDistanceKM / EARTH_RADIUS_KM;

    double minLat = originLatRadians - angle;
    double maxLat = originLatRadians + angle;

    double minLng;
    double maxLng;
    if (minLat > SloppyMath.toRadians(-90) && maxLat < SloppyMath.toRadians(90)) {
      double delta = Math.asin(Math.sin(angle)/Math.cos(originLatRadians));
      minLng = originLngRadians - delta;
      if (minLng < SloppyMath.toRadians(-180)) {
        minLng += 2 * Math.PI;
      }
      maxLng = originLngRadians + delta;
      if (maxLng > SloppyMath.toRadians(180)) {
        maxLng -= 2 * Math.PI;
      }
    } else {
      // The query includes a pole!
      minLat = Math.max(minLat, SloppyMath.toRadians(-90));
      maxLat = Math.min(maxLat, SloppyMath.toRadians(90));
      minLng = SloppyMath.toRadians(-180);
      maxLng = SloppyMath.toRadians(180);
    }

    BooleanQuery.Builder f = new BooleanQuery.Builder();

    // Add latitude range filter:
    f.add(DoublePoint.newRangeQuery("latitude", SloppyMath.toDegrees(minLat), SloppyMath.toDegrees(maxLat)),
          BooleanClause.Occur.FILTER);

    // Add longitude range filter:
    if (minLng > maxLng) {
      // The bounding box crosses the international date
      // line:
      BooleanQuery.Builder lonF = new BooleanQuery.Builder();
      lonF.add(DoublePoint.newRangeQuery("longitude", SloppyMath.toDegrees(minLng), Double.POSITIVE_INFINITY),
               BooleanClause.Occur.SHOULD);
      lonF.add(DoublePoint.newRangeQuery("longitude", Double.NEGATIVE_INFINITY, SloppyMath.toDegrees(maxLng)),
               BooleanClause.Occur.SHOULD);
      f.add(lonF.build(), BooleanClause.Occur.MUST);
    } else {
      f.add(DoublePoint.newRangeQuery("longitude", SloppyMath.toDegrees(minLng), SloppyMath.toDegrees(maxLng)),
            BooleanClause.Occur.FILTER);
    }

    return f.build();
  }

  /** User runs a query and counts facets. */
  public FacetResult search() throws IOException {

    FacetsCollector fc = new FacetsCollector();

    searcher.search(new MatchAllDocsQuery(), fc);

    Facets facets = new DoubleRangeFacetCounts("field", getDistanceValueSource(), fc,
                                               getBoundingBoxQuery(ORIGIN_LATITUDE, ORIGIN_LONGITUDE, 10.0),
                                               ONE_KM,
                                               TWO_KM,
                                               FIVE_KM,
                                               TEN_KM);

    return facets.getTopChildren(10, "field");
  }

  /** User drills down on the specified range. */
  public TopDocs drillDown(DoubleRange range) throws IOException {

    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    DrillDownQuery q = new DrillDownQuery(null);
    final DoubleValuesSource vs = getDistanceValueSource();
    q.add("field", range.getQuery(getBoundingBoxQuery(ORIGIN_LATITUDE, ORIGIN_LONGITUDE, range.max), vs));
    DrillSideways ds = new DrillSideways(searcher, config, (TaxonomyReader) null) {
        @Override
        protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {        
          assert drillSideways.length == 1;
          return new DoubleRangeFacetCounts("field", vs, drillSideways[0], ONE_KM, TWO_KM, FIVE_KM, TEN_KM);
        }
      };
    return ds.search(q, 10).hits;
  }

  @Override
  public void close() throws IOException {
    searcher.getIndexReader().close();
    indexDir.close();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    DistanceFacetsExample example = new DistanceFacetsExample();
    example.index();

    System.out.println("Distance facet counting example:");
    System.out.println("-----------------------");
    System.out.println(example.search());

    System.out.println("Distance facet drill-down example (field/< 2 km):");
    System.out.println("---------------------------------------------");
    TopDocs hits = example.drillDown(example.TWO_KM);
    System.out.println(hits.totalHits + " totalHits");

    example.close();
  }
}
