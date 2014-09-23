package org.apache.solr.schema;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.spatial4j.core.shape.Point;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Rectangle;

import org.apache.solr.util.SpatialUtils;


/**
 * Represents a Latitude/Longitude as a 2 dimensional point.  Latitude is <b>always</b> specified first.
 */
public class LatLonType extends AbstractSubTypeFieldType implements SpatialQueryable {
  protected static final int LAT = 0;
  protected static final int LON = 1;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    //TODO: refactor this, as we are creating the suffix cache twice, since the super.init does it too
    createSuffixCache(3);//we need three extra fields: one for the storage field, two for the lat/lon
  }

  @Override
  public List<StorableField> createFields(SchemaField field, Object value, float boost) {
    String externalVal = value.toString();
    //we could have 3 fields (two for the lat & lon, one for storage)
    List<StorableField> f = new ArrayList<>(3);
    if (field.indexed()) {
      Point point = SpatialUtils.parsePointSolrException(externalVal, SpatialContext.GEO);
      //latitude
      SchemaField subLatSF = subField(field, LAT, schema);
      f.add(subLatSF.createField(String.valueOf(point.getY()), subLatSF.indexed() && !subLatSF.omitNorms() ? boost : 1f));
      //longitude
      SchemaField subLonSF = subField(field, LON, schema);
      f.add(subLonSF.createField(String.valueOf(point.getX()), subLonSF.indexed() && !subLonSF.omitNorms() ? boost : 1f));
    }

    if (field.stored()) {
      FieldType customType = new FieldType();
      customType.setStored(true);
      f.add(createField(field.getName(), externalVal, customType, 1f));
    }
    return f;
  }


  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    Point p1 = SpatialUtils.parsePointSolrException(part1, SpatialContext.GEO);
    Point p2 = SpatialUtils.parsePointSolrException(part2, SpatialContext.GEO);

    SchemaField latSF = subField(field, LAT, parser.getReq().getSchema());
    SchemaField lonSF = subField(field, LON, parser.getReq().getSchema());
    BooleanQuery result = new BooleanQuery(true);
    // points must currently be ordered... should we support specifying any two opposite corner points?
    result.add(latSF.getType().getRangeQuery(parser, latSF,
        Double.toString(p1.getY()), Double.toString(p2.getY()), minInclusive, maxInclusive), BooleanClause.Occur.MUST);
    result.add(lonSF.getType().getRangeQuery(parser, lonSF,
        Double.toString(p1.getX()), Double.toString(p2.getX()), minInclusive, maxInclusive), BooleanClause.Occur.MUST);
    return result;
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    Point p1 = SpatialUtils.parsePointSolrException(externalVal, SpatialContext.GEO);

    SchemaField latSF = subField(field, LAT, parser.getReq().getSchema());
    SchemaField lonSF = subField(field, LON, parser.getReq().getSchema());
    BooleanQuery result = new BooleanQuery(true);
    result.add(latSF.getType().getFieldQuery(parser, latSF,
        Double.toString(p1.getY())), BooleanClause.Occur.MUST);
    result.add(lonSF.getType().getFieldQuery(parser, lonSF,
        Double.toString(p1.getX())), BooleanClause.Occur.MUST);
    return result;
  }



  @Override
  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    Point point = SpatialUtils.parsePointSolrException(options.pointStr, SpatialContext.GEO);

    // lat & lon in degrees
    double latCenter = point.getY();
    double lonCenter = point.getX();
    
    double distDeg = DistanceUtils.dist2Degrees(options.distance, options.radius);
    Rectangle bbox = DistanceUtils.calcBoxByDistFromPtDEG(latCenter, lonCenter, distDeg, SpatialContext.GEO, null);
    double latMin = bbox.getMinY();
    double latMax = bbox.getMaxY();
    double lonMin, lonMax, lon2Min, lon2Max;
    if (bbox.getCrossesDateLine()) {
       lonMin = -180;
       lonMax = bbox.getMaxX();
       lon2Min = bbox.getMinX();
       lon2Max = 180;
    } else {
       lonMin = bbox.getMinX();
       lonMax = bbox.getMaxX();
       lon2Min = -180;
       lon2Max = 180;
    }
    
    IndexSchema schema = parser.getReq().getSchema();
    
    // Now that we've figured out the ranges, build them!
    SchemaField latSF = subField(options.field, LAT, schema);
    SchemaField lonSF = subField(options.field, LON, schema);

    SpatialDistanceQuery spatial = new SpatialDistanceQuery();


    if (options.bbox) {
      BooleanQuery result = new BooleanQuery();

      Query latRange = latSF.getType().getRangeQuery(parser, latSF,
                String.valueOf(latMin),
                String.valueOf(latMax),
                true, true);
      result.add(latRange, BooleanClause.Occur.MUST);

      if (lonMin != -180 || lonMax != 180) {
        Query lonRange = lonSF.getType().getRangeQuery(parser, lonSF,
                String.valueOf(lonMin),
                String.valueOf(lonMax),
                true, true);
        if (lon2Min != -180 || lon2Max != 180) {
          // another valid longitude range
          BooleanQuery bothLons = new BooleanQuery();
          bothLons.add(lonRange, BooleanClause.Occur.SHOULD);

          lonRange = lonSF.getType().getRangeQuery(parser, lonSF,
                String.valueOf(lon2Min),
                String.valueOf(lon2Max),
                true, true);
          bothLons.add(lonRange, BooleanClause.Occur.SHOULD);

          lonRange = bothLons;
        }

        result.add(lonRange, BooleanClause.Occur.MUST);
      }

      spatial.bboxQuery = result;
    }


    spatial.origField = options.field.getName();
    spatial.latSource = latSF.getType().getValueSource(latSF, parser);
    spatial.lonSource = lonSF.getType().getValueSource(lonSF, parser);
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
    ArrayList<ValueSource> vs = new ArrayList<>(2);
    for (int i = 0; i < 2; i++) {
      SchemaField sub = subField(field, i, parser.getReq().getSchema());
      vs.add(sub.getType().getValueSource(sub, parser));
    }
    return new LatLonValueSource(field, vs);
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on LatLonType " + field.getName());
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }



  //It never makes sense to create a single field, so make it impossible to happen

  @Override
  public StorableField createField(SchemaField field, Object value, float boost) {
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

class SpatialDistanceQuery extends ExtendedQueryBase implements PostFilter {
  String origField;
  ValueSource latSource;
  ValueSource lonSource;
  double lonMin, lonMax, lon2Min, lon2Max, latMin, latMax;
  boolean lon2;

  boolean calcDist;  // actually calculate the distance with haversine
  Query bboxQuery;

  double latCenter;
  double lonCenter;
  double dist;
  double planetRadius;


  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return bboxQuery != null ? bboxQuery.rewrite(reader) : this;
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
      this.latContext = ValueSource.newContext(searcher);
      this.lonContext = ValueSource.newContext(searcher);
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
    public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
      return new SpatialScorer(context, acceptDocs, this, queryWeight);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return ((SpatialScorer)scorer(context, context.reader().getLiveDocs())).explain(doc);
    }
  }

  protected class SpatialScorer extends Scorer {
    final IndexReader reader;
    final SpatialWeight weight;
    final int maxDoc;
    final float qWeight;
    int doc=-1;
    final FunctionValues latVals;
    final FunctionValues lonVals;
    final Bits acceptDocs;


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

    public SpatialScorer(LeafReaderContext readerContext, Bits acceptDocs, SpatialWeight w, float qWeight) throws IOException {
      super(w);
      this.weight = w;
      this.qWeight = qWeight;
      this.reader = readerContext.reader();
      this.maxDoc = reader.maxDoc();
      this.acceptDocs = acceptDocs;
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
        if (acceptDocs != null && !acceptDocs.get(doc)) continue;
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

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public long cost() {
      return maxDoc;
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
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    try {
      return new SpatialCollector(new SpatialWeight(searcher));
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }


  class SpatialCollector extends DelegatingCollector {
    final SpatialWeight weight;
    SpatialScorer spatialScorer;
    int maxdoc;
    
    public SpatialCollector(SpatialWeight weight) {
      this.weight = weight;
    }

    @Override
    public void collect(int doc) throws IOException {
      spatialScorer.doc = doc;
      if (spatialScorer.match()) leafDelegate.collect(doc);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      maxdoc = context.reader().maxDoc();
      spatialScorer = new SpatialScorer(context, null, weight, 1.0f);
    }
  }


  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    // if we were supposed to use bboxQuery, then we should have been rewritten using that query
    assert bboxQuery == null;
    return new SpatialWeight(searcher);
  }


  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field)
  {
    float boost = getBoost();
    return super.getOptions() + (boost!=1.0?"(":"") +
            (calcDist ? "geofilt" : "bbox") + "(latlonSource="+origField +"(" + latSource + "," + lonSource + ")"
            +",latCenter="+latCenter+",lonCenter="+lonCenter
            +",dist=" + dist
            +",latMin=" + latMin + ",latMax="+latMax
            +",lonMin=" + lonMin + ",lonMax"+lonMax
            +",lon2Min=" + lon2Min + ",lon2Max" + lon2Max
            +",calcDist="+calcDist
            +",planetRadius="+planetRadius
            // + (bboxQuery == null ? "" : ",bboxQuery="+bboxQuery)
            +")"
            + (boost==1.0 ? "" : ")^"+boost);
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
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
    // don't bother making the hash expensive - the center latitude + min longitude will be very unique
    long hash = Double.doubleToLongBits(latCenter);
    hash = hash * 31 + Double.doubleToLongBits(lonMin);
    hash = hash * 31 + (long)super.hashCode();
    return (int)(hash >> 32 + hash);
  }

}


