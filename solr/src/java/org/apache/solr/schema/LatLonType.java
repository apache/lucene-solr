package org.apache.solr.schema;
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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.DistanceUtils;
import org.apache.lucene.spatial.tier.InvalidGeoException;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Represents a Latitude/Longitude as a 2 dimensional point.  Latitude is <b>always</b> specified first.
 */
public class LatLonType extends AbstractSubTypeFieldType implements SpatialQueryable {
  protected static final int LAT = 0;
  protected static final int LONG = 1;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    //TODO: refactor this, as we are creating the suffix cache twice, since the super.init does it too
    createSuffixCache(3);//we need three extra fields: one for the storage field, two for the lat/lon
  }

  @Override
  public Fieldable[] createFields(SchemaField field, Object value, float boost) {
    String externalVal = value.toString();
    //we could have tileDiff + 3 fields (two for the lat/lon, one for storage)
    Fieldable[] f = new Fieldable[(field.indexed() ? 2 : 0) + (field.stored() ? 1 : 0)];
    if (field.indexed()) {
      int i = 0;
      double[] latLon = new double[0];
      try {
        latLon = DistanceUtils.parseLatitudeLongitude(null, externalVal);
      } catch (InvalidGeoException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      //latitude
      f[i] = subField(field, i).createField(String.valueOf(latLon[LAT]), boost);
      i++;
      //longitude
      f[i] = subField(field, i).createField(String.valueOf(latLon[LONG]), boost);

    }

    if (field.stored()) {
      f[f.length - 1] = createField(field.getName(), externalVal,
              getFieldStore(field, externalVal), Field.Index.NO, Field.TermVector.NO,
              false, false, boost);
    }
    return f;
  }


  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    int dimension = 2;

    String[] p1;
    String[] p2;
    try {
      p1 = DistanceUtils.parsePoint(null, part1, dimension);
      p2 = DistanceUtils.parsePoint(null, part2, dimension);
    } catch (InvalidGeoException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
    BooleanQuery result = new BooleanQuery(true);
    for (int i = 0; i < dimension; i++) {
      SchemaField subSF = subField(field, i);
      // points must currently be ordered... should we support specifying any two opposite corner points?
      result.add(subSF.getType().getRangeQuery(parser, subSF, p1[i], p2[i], minInclusive, maxInclusive), BooleanClause.Occur.MUST);
    }
    return result;

  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    int dimension = 2;
    
    String[] p1 = new String[0];
    try {
      p1 = DistanceUtils.parsePoint(null, externalVal, dimension);
    } catch (InvalidGeoException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
    BooleanQuery bq = new BooleanQuery(true);
    for (int i = 0; i < dimension; i++) {
      SchemaField sf = subField(field, i);
      Query tq = sf.getType().getFieldQuery(parser, sf, p1[i]);
      bq.add(tq, BooleanClause.Occur.MUST);
    }
    return bq;
  }



  @Override
  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    double[] point = null;
    try {
      point = DistanceUtils.parseLatitudeLongitude(options.pointStr);
    } catch (InvalidGeoException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    // lat & lon in degrees
    double latCenter = point[LAT];
    double lonCenter = point[LONG];

    point[0] = point[0] * DistanceUtils.DEGREES_TO_RADIANS;
    point[1] = point[1] * DistanceUtils.DEGREES_TO_RADIANS;
    //Get the distance

    double[] tmp = new double[2];
    //these calculations aren't totally accurate, but it should be good enough
    //TODO: Optimize to do in single calculations.  Would need to deal with poles, prime meridian, etc.
    double [] north = DistanceUtils.pointOnBearing(point[LAT], point[LONG], options.distance, 0, tmp, options.radius);
    //This returns the point as radians, but we need degrees b/c that is what the field is stored as
    double ur_lat = north[LAT] * DistanceUtils.RADIANS_TO_DEGREES;//get it now, as we are going to reuse tmp
    double [] east = DistanceUtils.pointOnBearing(point[LAT], point[LONG], options.distance, DistanceUtils.DEG_90_AS_RADS, tmp, options.radius);
    double ur_lon = east[LONG] * DistanceUtils.RADIANS_TO_DEGREES;
    double [] south = DistanceUtils.pointOnBearing(point[LAT], point[LONG], options.distance, DistanceUtils.DEG_180_AS_RADS, tmp, options.radius);
    double ll_lat = south[LAT] * DistanceUtils.RADIANS_TO_DEGREES;
    double [] west = DistanceUtils.pointOnBearing(point[LAT], point[LONG], options.distance, DistanceUtils.DEG_270_AS_RADS, tmp, options.radius);
    double ll_lon = west[LONG] * DistanceUtils.RADIANS_TO_DEGREES;
    

    //TODO: can we reuse our bearing calculations?
    double angDist = DistanceUtils.angularDistance(options.distance,
            options.radius);//in radians

    double latMin = -90.0, latMax = 90.0, lonMin = -180.0, lonMax = 180.0;
    double lon2Min = -180.0, lon2Max = 180.0;  // optional second longitude restriction

    // for the poles, do something slightly different - a polar "cap".
    // Also, note point[LAT] is in radians, but ur and ll are in degrees
    if (point[LAT] + angDist > DistanceUtils.DEG_90_AS_RADS) { // we cross the north pole
      //we don't need a longitude boundary at all
      latMin = Math.min(ll_lat, ur_lat);
    } else if (point[LAT] - angDist < -DistanceUtils.DEG_90_AS_RADS) { // we cross the south pole
      latMax = Math.max(ll_lat, ur_lat);
    } else {
      // set the latitude restriction as normal
      latMin = ll_lat;
      latMax = ur_lat;

      if (ll_lon > ur_lon) {
         // we crossed the +-180 deg longitude... need to make
        // range queries of (-180 TO ur) OR (ll TO 180)
        lonMin = -180;
        lonMax = ur_lon;
        lon2Min = ll_lon;
        lon2Max = 180;
      } else {
        lonMin = ll_lon;
        lonMax = ur_lon;
      }
    }


    // Now that we've figured out the ranges, build them!
    SchemaField latField = subField(options.field, LAT);
    SchemaField lonField = subField(options.field, LONG);

    if (options.bbox) {
      BooleanQuery result = new BooleanQuery();  // only used if box==true

      Query latRange = latField.getType().getRangeQuery(parser, latField,
                String.valueOf(latMin),
                String.valueOf(latMax),
                true, true);
      result.add(latRange, BooleanClause.Occur.MUST);

      if (lonMin != -180 || lonMax != 180) {
        Query lonRange = lonField.getType().getRangeQuery(parser, lonField,
                String.valueOf(lonMin),
                String.valueOf(lonMax),
                true, true);
        if (lon2Min != -180 || lon2Max != 180) {
          // another valid longitude range
          BooleanQuery bothLons = new BooleanQuery();
          bothLons.add(lonRange, BooleanClause.Occur.SHOULD);

          lonRange = lonField.getType().getRangeQuery(parser, lonField,
                String.valueOf(lon2Min),
                String.valueOf(lon2Max),
                true, true);
          bothLons.add(lonRange, BooleanClause.Occur.SHOULD);

          lonRange = bothLons;
        }

        result.add(lonRange, BooleanClause.Occur.MUST);
      }

      return result;
    }


    SpatialDistanceQuery spatial = new SpatialDistanceQuery();
    spatial.origField = options.field.getName();
    spatial.latSource = latField.getType().getValueSource(latField, parser);
    spatial.lonSource = lonField.getType().getValueSource(lonField, parser);
    spatial.latMin = latMin;
    spatial.latMax = latMax;
    spatial.lonMin = lonMin;
    spatial.lonMax = lonMax;
    spatial.lon2Min = lon2Min;
    spatial.lon2Max = lon2Max;
    spatial.lon2 = lon2Min != -180 || lon2Max != 180;

    spatial.latCenter = latCenter;
    spatial.lonCenter = lonCenter;
    spatial.dist = options.distance;
    spatial.planetRadius = options.radius;

    spatial.calcDist = !options.bbox;

    return spatial;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    ArrayList<ValueSource> vs = new ArrayList<ValueSource>(2);
    for (int i = 0; i < 2; i++) {
      SchemaField sub = subField(field, i);
      vs.add(sub.getType().getValueSource(sub, parser));
    }
    return new LatLonValueSource(field, vs);
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeStr(name, f.stringValue(), false);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on LatLonType " + field.getName());
  }



  //It never makes sense to create a single field, so make it impossible to happen

  @Override
  public Fieldable createField(SchemaField field, Object value, float boost) {
    throw new UnsupportedOperationException("LatLonType uses multiple fields.  field=" + field.getName());
  }

}

class LatLonValueSource extends VectorValueSource {
  private final SchemaField sf;

  public LatLonValueSource(SchemaField sf, List<ValueSource> sources) {
    super(sources);
    this.sf = sf;
  }

  @Override
  public String name() {
    return "latlon";
  }

  @Override
  public String description() {
    return name() + "(" + sf.getName() + ")";
  }
}


////////////////////////////////////////////////////////////////////////////////////////////
// TODO: recast as a value source that doesn't have to match all docs

class SpatialDistanceQuery extends Query {
  String origField;
  ValueSource latSource;
  ValueSource lonSource;
  double lonMin, lonMax, lon2Min, lon2Max, latMin, latMax;
  boolean lon2;

  boolean calcDist;  // actually calculate the distance with haversine

  double latCenter;
  double lonCenter;
  double dist;
  double planetRadius;


  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  @Override
  public void extractTerms(Set terms) {}

  protected class SpatialWeight extends Weight {
    protected IndexSearcher searcher;
    protected float queryNorm;
    protected float queryWeight;
    protected Map latContext;
    protected Map lonContext;

    public SpatialWeight(IndexSearcher searcher) throws IOException {
      this.searcher = searcher;
      this.latContext = latSource.newContext(searcher);
      this.lonContext = lonSource.newContext(searcher);
      latSource.createWeight(latContext, searcher);
      lonSource.createWeight(lonContext, searcher);
    }

    @Override
    public Query getQuery() {
      return SpatialDistanceQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, ScorerContext scorerContext) throws IOException {
      return new SpatialScorer(context, this, queryWeight);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      return ((SpatialScorer)scorer(context, ScorerContext.def().scoreDocsInOrder(true).topScorer(true))).explain(doc);
    }
  }

  protected class SpatialScorer extends Scorer {
    final IndexReader reader;
    final SpatialWeight weight;
    final int maxDoc;
    final float qWeight;
    int doc=-1;
    final DocValues latVals;
    final DocValues lonVals;
    final Bits liveDocs;


    final double lonMin, lonMax, lon2Min, lon2Max, latMin, latMax;
    final boolean lon2;
    final boolean calcDist;
    
    final double latCenterRad;
    final double lonCenterRad;
    final double latCenterRad_cos;
    final double dist;
    final double planetRadius;

    int lastDistDoc;
    double lastDist;

    public SpatialScorer(AtomicReaderContext readerContext, SpatialWeight w, float qWeight) throws IOException {
      super(w);
      this.weight = w;
      this.qWeight = qWeight;
      this.reader = readerContext.reader;
      this.maxDoc = reader.maxDoc();
      this.liveDocs = reader.getLiveDocs();
      latVals = latSource.getValues(weight.latContext, readerContext);
      lonVals = lonSource.getValues(weight.lonContext, readerContext);

      this.lonMin = SpatialDistanceQuery.this.lonMin;
      this.lonMax = SpatialDistanceQuery.this.lonMax;
      this.lon2Min = SpatialDistanceQuery.this.lon2Min;
      this.lon2Max = SpatialDistanceQuery.this.lon2Max;
      this.latMin = SpatialDistanceQuery.this.latMin;
      this.latMax = SpatialDistanceQuery.this.latMax;
      this.lon2 = SpatialDistanceQuery.this.lon2;
      this.calcDist = SpatialDistanceQuery.this.calcDist;

      this.latCenterRad = SpatialDistanceQuery.this.latCenter * DistanceUtils.DEGREES_TO_RADIANS;
      this.lonCenterRad = SpatialDistanceQuery.this.lonCenter * DistanceUtils.DEGREES_TO_RADIANS;
      this.latCenterRad_cos = this.calcDist ? Math.cos(latCenterRad) : 0;
      this.dist = SpatialDistanceQuery.this.dist;
      this.planetRadius = SpatialDistanceQuery.this.planetRadius;

    }

    boolean match() {
      // longitude should generally be more restrictive than latitude
      // (e.g. in the US, it immediately separates the coasts, and in world search separates
      // US from Europe from Asia, etc.
      double lon = lonVals.doubleVal(doc);
      if (! ((lon >= lonMin && lon <=lonMax) || (lon2 && lon >= lon2Min && lon <= lon2Max)) ) {
        return false;
      }

      double lat = latVals.doubleVal(doc);
      if ( !(lat >= latMin && lat <= latMax) ) {
        return false;
      }

      if (!calcDist) return true;

      // TODO: test for internal box where we wouldn't need to calculate the distance

      return dist(lat, lon) <= dist;
    }

    double dist(double lat, double lon) {
      double latRad = lat * DistanceUtils.DEGREES_TO_RADIANS;
      double lonRad = lon * DistanceUtils.DEGREES_TO_RADIANS;
      
      // haversine, specialized to avoid a cos() call on latCenterRad
      double diffX = latCenterRad - latRad;
      double diffY = lonCenterRad - lonRad;
      double hsinX = Math.sin(diffX * 0.5);
      double hsinY = Math.sin(diffY * 0.5);
      double h = hsinX * hsinX +
              (latCenterRad_cos * Math.cos(latRad) * hsinY * hsinY);
      double result = (planetRadius * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h)));

      // save the results of this calculation
      lastDistDoc = doc;
      lastDist = result;
      
      return result;
    }

    @Override
    public int docID() {
      return doc;
    }

    // instead of matching all docs, we could also embed a query.
    // the score could either ignore the subscore, or boost it.
    // Containment:  floatline(foo:myTerm, "myFloatField", 1.0, 0.0f)
    // Boost:        foo:myTerm^floatline("myFloatField",1.0,0.0f)
    @Override
    public int nextDoc() throws IOException {
      for(;;) {
        ++doc;
        if (doc>=maxDoc) {
          return doc=NO_MORE_DOCS;
        }
        if (liveDocs != null && !liveDocs.get(doc)) continue;
        if (!match()) continue;
        return doc;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // this will work even if target==NO_MORE_DOCS
      doc=target-1;
      return nextDoc();
    }

    @Override
    public float score() throws IOException {
      double dist = (doc == lastDistDoc) ? lastDist : dist(latVals.doubleVal(doc), lonVals.doubleVal(doc));
      return (float)(dist * qWeight);
    }

    public Explanation explain(int doc) throws IOException {
      advance(doc);
      boolean matched = this.doc == doc;
      this.doc = doc;

      float sc = matched ? score() : 0;
      double dist = dist(latVals.doubleVal(doc), lonVals.doubleVal(doc));

      String description = SpatialDistanceQuery.this.toString();

      Explanation result = new ComplexExplanation
        (this.doc == doc, sc, description +  " product of:");
      // result.addDetail(new Explanation((float)dist, "hsin("+latVals.explain(doc)+","+lonVals.explain(doc)));
      result.addDetail(new Explanation((float)dist, "hsin("+latVals.doubleVal(doc)+","+lonVals.doubleVal(doc)));
      result.addDetail(new Explanation(getBoost(), "boost"));
      result.addDetail(new Explanation(weight.queryNorm,"queryNorm"));
      return result;
    }
  }


  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new SpatialWeight(searcher);
  }


  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field)
  {
    float boost = getBoost();
    return (boost!=1.0?"(":"") +
            "geofilt(latlonSource="+origField +"(" + latSource + "," + lonSource + ")"
            +",latCenter="+latCenter+",lonCenter="+lonCenter
            +",dist=" + dist
            +",latMin=" + latMin + ",latMax="+latMax
            +",lonMin=" + lonMin + ",lonMax"+lonMax
            +",lon2Min=" + lon2Min + ",lon2Max" + lon2Max
            +",calcDist="+calcDist
            +",planetRadius="+planetRadius
            +")"
            + (boost==1.0 ? "" : ")^"+boost);
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (SpatialDistanceQuery.class != o.getClass()) return false;
    SpatialDistanceQuery other = (SpatialDistanceQuery)o;
    return     this.latCenter == other.latCenter
            && this.lonCenter == other.lonCenter
            && this.latMin == other.latMin
            && this.latMax == other.latMax
            && this.lonMin == other.lonMin
            && this.lonMax == other.lonMax
            && this.lon2Min == other.lon2Min
            && this.lon2Max == other.lon2Max
            && this.dist == other.dist
            && this.planetRadius == other.planetRadius
            && this.calcDist == other.calcDist
            && this.lonSource.equals(other.lonSource)
            && this.latSource.equals(other.latSource)
            && this.getBoost() == other.getBoost()
        ;
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    // don't bother making the hash expensive - the center latitude + min longitude will be very uinque 
    long hash = Double.doubleToLongBits(latCenter);
    hash = hash * 31 + Double.doubleToLongBits(lonMin);
    return (int)(hash >> 32 + hash);
  }

}


