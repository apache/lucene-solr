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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.util.SpatialUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;


/**
 * Represents a Latitude/Longitude as a 2 dimensional point.  Latitude is <b>always</b> specified first.
 *
 * @deprecated use {@link LatLonPointSpatialField} instead
 */
@Deprecated
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
  public List<IndexableField> createFields(SchemaField field, Object value) {
    String externalVal = value.toString();
    //we could have 3 fields (two for the lat & lon, one for storage)
    List<IndexableField> f = new ArrayList<>(3);
    if (field.indexed()) {
      Point point = SpatialUtils.parsePointSolrException(externalVal, SpatialContext.GEO);
      //latitude
      SchemaField subLatSF = subField(field, LAT, schema);
      f.addAll(subLatSF.createFields(String.valueOf(point.getY())));
      //longitude
      SchemaField subLonSF = subField(field, LON, schema);
      f.addAll(subLonSF.createFields(String.valueOf(point.getX())));
    }

    if (field.stored()) {
      f.add(createField(field.getName(), externalVal, StoredField.TYPE));
    }
    return f;
  }
  
  @Override
  protected void checkSupportsDocValues() {
    // DocValues supported only when enabled at the fieldType 
    if (!hasProperty(DOC_VALUES)) {
      throw new UnsupportedOperationException("LatLonType can't have docValues=true in the field definition, use docValues=true in the fieldType definition, or in subFieldType/subFieldSuffix");
    }
  }


  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    Point p1 = SpatialUtils.parsePointSolrException(part1, SpatialContext.GEO);
    Point p2 = SpatialUtils.parsePointSolrException(part2, SpatialContext.GEO);

    SchemaField latSF = subField(field, LAT, parser.getReq().getSchema());
    SchemaField lonSF = subField(field, LON, parser.getReq().getSchema());
    BooleanQuery.Builder result = new BooleanQuery.Builder();
    // points must currently be ordered... should we support specifying any two opposite corner points?
    result.add(latSF.getType().getRangeQuery(parser, latSF,
        Double.toString(p1.getY()), Double.toString(p2.getY()), minInclusive, maxInclusive), BooleanClause.Occur.MUST);
    result.add(lonSF.getType().getRangeQuery(parser, lonSF,
        Double.toString(p1.getX()), Double.toString(p2.getX()), minInclusive, maxInclusive), BooleanClause.Occur.MUST);
    return result.build();
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    Point p1 = SpatialUtils.parsePointSolrException(externalVal, SpatialContext.GEO);

    SchemaField latSF = subField(field, LAT, parser.getReq().getSchema());
    SchemaField lonSF = subField(field, LON, parser.getReq().getSchema());
    BooleanQuery.Builder result = new BooleanQuery.Builder();
    result.add(latSF.getType().getFieldQuery(parser, latSF,
        Double.toString(p1.getY())), BooleanClause.Occur.MUST);
    result.add(lonSF.getType().getFieldQuery(parser, lonSF,
        Double.toString(p1.getX())), BooleanClause.Occur.MUST);
    return result.build();
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
      BooleanQuery.Builder result = new BooleanQuery.Builder();

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
          BooleanQuery.Builder bothLons = new BooleanQuery.Builder();
          bothLons.add(lonRange, BooleanClause.Occur.SHOULD);

          lonRange = lonSF.getType().getRangeQuery(parser, lonSF,
                String.valueOf(lon2Min),
                String.valueOf(lon2Max),
                true, true);
          bothLons.add(lonRange, BooleanClause.Occur.SHOULD);

          lonRange = bothLons.build();
        }

        result.add(lonRange, BooleanClause.Occur.MUST);
      }

      spatial.bboxQuery = result.build();
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
      SchemaField sub = subField(field, i, schema);
      vs.add(sub.getType().getValueSource(sub, parser));
    }
    return new LatLonValueSource(field, vs);
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
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
  public IndexableField createField(SchemaField field, Object value) {
    throw new UnsupportedOperationException("LatLonType uses multiple fields.  field=" + field.getName());
  }

  @Override
  public double getSphereRadius() {
    return DistanceUtils.EARTH_MEAN_RADIUS_KM;
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

  protected class SpatialWeight extends ConstantScoreWeight {
    protected IndexSearcher searcher;
    @SuppressWarnings({"rawtypes"})
    protected Map latContext;
    @SuppressWarnings({"rawtypes"})
    protected Map lonContext;

    @SuppressWarnings({"unchecked"})
    public SpatialWeight(IndexSearcher searcher, float boost) throws IOException {
      super(SpatialDistanceQuery.this, boost);
      this.searcher = searcher;
      this.latContext = ValueSource.newContext(searcher);
      this.lonContext = ValueSource.newContext(searcher);
      latSource.createWeight(latContext, searcher);
      lonSource.createWeight(lonContext, searcher);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return new SpatialScorer(context, this, score());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return ((SpatialScorer)scorer(context)).explain(super.explain(context, doc), doc);
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

    @SuppressWarnings({"unchecked"})
    public SpatialScorer(LeafReaderContext readerContext, SpatialWeight w, float qWeight) throws IOException {
      super(w);
      this.weight = w;
      this.qWeight = qWeight;
      this.reader = readerContext.reader();
      this.maxDoc = reader.maxDoc();
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

    boolean match() throws IOException {
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

    @Override
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {

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
        public long cost() {
          return maxDoc;
        }

      };
    }

    @Override
    public float score() throws IOException {
      double dist = (doc == lastDistDoc) ? lastDist : dist(latVals.doubleVal(doc), lonVals.doubleVal(doc));
      return (float)(dist * qWeight);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }

    public Explanation explain(Explanation base, int doc) throws IOException {
      if (base.isMatch() == false) {
        return base;
      }
      double dist = dist(latVals.doubleVal(doc), lonVals.doubleVal(doc));

      String description = SpatialDistanceQuery.this.toString();
      return Explanation.match((float) (base.getValue().floatValue() * dist), description + " product of:",
          base, Explanation.match((float) dist, "hsin("+latVals.doubleVal(doc)+","+lonVals.doubleVal(doc)));
    }

  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    try {
      return new SpatialCollector(new SpatialWeight(searcher, 1f));
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
      spatialScorer = new SpatialScorer(context, weight, 1.0f);
    }
  }


  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    // if we were supposed to use bboxQuery, then we should have been rewritten using that query
    assert bboxQuery == null;
    return new SpatialWeight(searcher, boost);
  }


  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field)
  {
    return super.getOptions() +
            (calcDist ? "geofilt" : "bbox") + "(latlonSource="+origField +"(" + latSource + "," + lonSource + ")"
            +",latCenter="+latCenter+",lonCenter="+lonCenter
            +",dist=" + dist
            +",latMin=" + latMin + ",latMax="+latMax
            +",lonMin=" + lonMin + ",lonMax"+lonMax
            +",lon2Min=" + lon2Min + ",lon2Max" + lon2Max
            +",calcDist="+calcDist
            +",planetRadius="+planetRadius
            // + (bboxQuery == null ? "" : ",bboxQuery="+bboxQuery)
            +")";
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!sameClassAs(o)) return false;
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
        ;
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    // don't bother making the hash expensive - the center latitude + min longitude will be very unique
    long hash = classHash();
    hash = hash * 31 + Double.doubleToLongBits(latCenter);
    hash = hash * 31 + Double.doubleToLongBits(lonMin);
    return (int) (hash >> 32 + hash);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}


