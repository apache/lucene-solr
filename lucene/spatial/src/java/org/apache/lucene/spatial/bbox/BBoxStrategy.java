package org.apache.lucene.spatial.bbox;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;


/**
 * Based on GeoPortal's
 * <a href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialClauseAdapter.java">SpatialClauseAdapter</a>.
 *
 * @lucene.experimental
 */
public class BBoxStrategy extends SpatialStrategy {

  public static final String SUFFIX_MINX = "__minX";
  public static final String SUFFIX_MAXX = "__maxX";
  public static final String SUFFIX_MINY = "__minY";
  public static final String SUFFIX_MAXY = "__maxY";
  public static final String SUFFIX_XDL  = "__xdl";

  /*
   * The Bounding Box gets stored as four fields for x/y min/max and a flag
   * that says if the box crosses the dateline (xdl).
   */
  public final String field_bbox;
  public final String field_minX;
  public final String field_minY;
  public final String field_maxX;
  public final String field_maxY;
  public final String field_xdl; // crosses dateline

  public double queryPower = 1.0;
  public double targetPower = 1.0f;
  public int precisionStep = 8; // same as solr default

  public BBoxStrategy(SpatialContext ctx, String fieldNamePrefix) {
    super(ctx, fieldNamePrefix);
    field_bbox = fieldNamePrefix;
    field_minX = fieldNamePrefix + SUFFIX_MINX;
    field_maxX = fieldNamePrefix + SUFFIX_MAXX;
    field_minY = fieldNamePrefix + SUFFIX_MINY;
    field_maxY = fieldNamePrefix + SUFFIX_MAXY;
    field_xdl = fieldNamePrefix + SUFFIX_XDL;
  }

  public void setPrecisionStep( int p ) {
    precisionStep = p;
    if (precisionStep<=0 || precisionStep>=64)
      precisionStep=Integer.MAX_VALUE;
  }

  //---------------------------------
  // Indexing
  //---------------------------------

  @Override
  public Field[] createIndexableFields(Shape shape) {
    Rectangle bbox = shape.getBoundingBox();
    FieldType doubleFieldType = new FieldType(DoubleField.TYPE_NOT_STORED);
    doubleFieldType.setNumericPrecisionStep(precisionStep);
    Field[] fields = new Field[5];
    fields[0] = new DoubleField(field_minX, bbox.getMinX(), doubleFieldType);
    fields[1] = new DoubleField(field_maxX, bbox.getMaxX(), doubleFieldType);
    fields[2] = new DoubleField(field_minY, bbox.getMinY(), doubleFieldType);
    fields[3] = new DoubleField(field_maxY, bbox.getMaxY(), doubleFieldType);
    fields[4] = new Field( field_xdl, bbox.getCrossesDateLine()?"T":"F", StringField.TYPE_NOT_STORED);
    return fields;
  }

  //---------------------------------
  // Query Builder
  //---------------------------------

  @Override
  public ValueSource makeValueSource(SpatialArgs args) {
    return new BBoxSimilarityValueSource(
        this, new AreaSimilarity(args.getShape().getBoundingBox(), queryPower, targetPower));
  }


  @Override
  public Filter makeFilter(SpatialArgs args) {
    Query spatial = makeSpatialQuery(args);
    return new QueryWrapperFilter( spatial );
  }

  @Override
  public Query makeQuery(SpatialArgs args) {
    BooleanQuery bq = new BooleanQuery();
    Query spatial = makeSpatialQuery(args);
    bq.add(new ConstantScoreQuery(spatial), BooleanClause.Occur.MUST);
    
    // This part does the scoring
    Query spatialRankingQuery = new FunctionQuery(makeValueSource(args));
    bq.add(spatialRankingQuery, BooleanClause.Occur.MUST);
    return bq;
  }


  private Query makeSpatialQuery(SpatialArgs args) {
    Rectangle bbox = args.getShape().getBoundingBox();
    Query spatial = null;

    // Useful for understanding Relations:
    // http://edndoc.esri.com/arcsde/9.1/general_topics/understand_spatial_relations.htm
    SpatialOperation op = args.getOperation();
         if( op == SpatialOperation.BBoxIntersects ) spatial = makeIntersects(bbox);
    else if( op == SpatialOperation.BBoxWithin     ) spatial = makeWithin(bbox);
    else if( op == SpatialOperation.Contains       ) spatial = makeContains(bbox);
    else if( op == SpatialOperation.Intersects     ) spatial = makeIntersects(bbox);
    else if( op == SpatialOperation.IsEqualTo      ) spatial = makeEquals(bbox);
    else if( op == SpatialOperation.IsDisjointTo   ) spatial = makeDisjoint(bbox);
    else if( op == SpatialOperation.IsWithin       ) spatial = makeWithin(bbox);
    else if( op == SpatialOperation.Overlaps       ) spatial = makeIntersects(bbox);
    else {
        throw new UnsupportedSpatialOperation(op);
    }
    return spatial;
  }


  //-------------------------------------------------------------------------------
  //
  //-------------------------------------------------------------------------------

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   *
   * @return the spatial query
   */
  Query makeContains(Rectangle bbox) {

    // general case
    // docMinX <= queryExtent.getMinX() AND docMinY <= queryExtent.getMinY() AND docMaxX >= queryExtent.getMaxX() AND docMaxY >= queryExtent.getMaxY()

    // Y conditions
    // docMinY <= queryExtent.getMinY() AND docMaxY >= queryExtent.getMaxY()
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, precisionStep, null, bbox.getMinY(), false, true);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, precisionStep, bbox.getMaxY(), null, true, false);
    Query yConditions = this.makeQuery(new Query[]{qMinY, qMaxY}, BooleanClause.Occur.MUST);

    // X conditions
    Query xConditions = null;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // documents that contain the min X and max X of the query envelope,
      // docMinX <= queryExtent.getMinX() AND docMaxX >= queryExtent.getMaxX()
      Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, null, bbox.getMinX(), false, true);
      Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, bbox.getMaxX(), null, true, false);
      Query qMinMax = this.makeQuery(new Query[]{qMinX, qMaxX}, BooleanClause.Occur.MUST);
      Query qNonXDL = this.makeXDL(false, qMinMax);

      // X Conditions for documents that cross the date line,
      // the left portion of the document contains the min X of the query
      // OR the right portion of the document contains the max X of the query,
      // docMinXLeft <= queryExtent.getMinX() OR docMaxXRight >= queryExtent.getMaxX()
      Query qXDLLeft = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, null, bbox.getMinX(), false, true);
      Query qXDLRight = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, bbox.getMaxX(), null, true, false);
      Query qXDLLeftRight = this.makeQuery(new Query[]{qXDLLeft, qXDLRight}, BooleanClause.Occur.SHOULD);
      Query qXDL = this.makeXDL(true, qXDLLeftRight);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);

      // queries that cross the date line
    } else {

      // No need to search for documents that do not cross the date line

      // X Conditions for documents that cross the date line,
      // the left portion of the document contains the min X of the query
      // AND the right portion of the document contains the max X of the query,
      // docMinXLeft <= queryExtent.getMinX() AND docMaxXRight >= queryExtent.getMaxX()
      Query qXDLLeft = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, null, bbox.getMinX(), false, true);
      Query qXDLRight = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, bbox.getMaxX(), null, true, false);
      Query qXDLLeftRight = this.makeQuery(new Query[]{qXDLLeft, qXDLRight}, BooleanClause.Occur.MUST);

      xConditions = this.makeXDL(true, qXDLLeftRight);
    }

    // both X and Y conditions must occur
    return this.makeQuery(new Query[]{xConditions, yConditions}, BooleanClause.Occur.MUST);
  }

  /**
   * Constructs a query to retrieve documents that are disjoint to the input envelope.
   *
   * @return the spatial query
   */
  Query makeDisjoint(Rectangle bbox) {

    // general case
    // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX() OR docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()

    // Y conditions
    // docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, precisionStep, bbox.getMaxY(), null, false, false);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, precisionStep, null, bbox.getMinY(), false, false);
    Query yConditions = this.makeQuery(new Query[]{qMinY, qMaxY}, BooleanClause.Occur.SHOULD);

    // X conditions
    Query xConditions = null;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()
      Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMaxX(), null, false, false);
      Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, bbox.getMinX(), false, false);
      Query qMinMax = this.makeQuery(new Query[]{qMinX, qMaxX}, BooleanClause.Occur.SHOULD);
      Query qNonXDL = this.makeXDL(false, qMinMax);

      // X Conditions for documents that cross the date line,
      // both the left and right portions of the document must be disjoint to the query
      // (docMinXLeft > queryExtent.getMaxX() OR docMaxXLeft < queryExtent.getMinX()) AND
      // (docMinXRight > queryExtent.getMaxX() OR docMaxXRight < queryExtent.getMinX())
      // where: docMaxXLeft = 180.0, docMinXRight = -180.0
      // (docMaxXLeft  < queryExtent.getMinX()) equates to (180.0  < queryExtent.getMinX()) and is ignored
      // (docMinXRight > queryExtent.getMaxX()) equates to (-180.0 > queryExtent.getMaxX()) and is ignored
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMaxX(), null, false, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, bbox.getMinX(), false, false);
      Query qLeftRight = this.makeQuery(new Query[]{qMinXLeft, qMaxXRight}, BooleanClause.Occur.MUST);
      Query qXDL = this.makeXDL(true, qLeftRight);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);

      // queries that cross the date line
    } else {

      // X Conditions for documents that do not cross the date line,
      // the document must be disjoint to both the left and right query portions
      // (docMinX > queryExtent.getMaxX()Left OR docMaxX < queryExtent.getMinX()) AND (docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()Left)
      // where: queryExtent.getMaxX()Left = 180.0, queryExtent.getMinX()Left = -180.0
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, 180.0, null, false, false);
      Query qMaxXLeft = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, bbox.getMinX(), false, false);
      Query qMinXRight = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMaxX(), null, false, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, -180.0, false, false);
      Query qLeft = this.makeQuery(new Query[]{qMinXLeft, qMaxXLeft}, BooleanClause.Occur.SHOULD);
      Query qRight = this.makeQuery(new Query[]{qMinXRight, qMaxXRight}, BooleanClause.Occur.SHOULD);
      Query qLeftRight = this.makeQuery(new Query[]{qLeft, qRight}, BooleanClause.Occur.MUST);

      // No need to search for documents that do not cross the date line

      xConditions = this.makeXDL(false, qLeftRight);
    }

    // either X or Y conditions should occur
    return this.makeQuery(new Query[]{xConditions, yConditions}, BooleanClause.Occur.SHOULD);
  }

  /**
   * Constructs a query to retrieve documents that equal the input envelope.
   *
   * @return the spatial query
   */
  Query makeEquals(Rectangle bbox) {

    // docMinX = queryExtent.getMinX() AND docMinY = queryExtent.getMinY() AND docMaxX = queryExtent.getMaxX() AND docMaxY = queryExtent.getMaxY()
    Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMinX(), bbox.getMinX(), true, true);
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, precisionStep, bbox.getMinY(), bbox.getMinY(), true, true);
    Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, bbox.getMaxX(), bbox.getMaxX(), true, true);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, precisionStep, bbox.getMaxY(), bbox.getMaxY(), true, true);
    BooleanQuery bq = new BooleanQuery();
    bq.add(qMinX, BooleanClause.Occur.MUST);
    bq.add(qMinY, BooleanClause.Occur.MUST);
    bq.add(qMaxX, BooleanClause.Occur.MUST);
    bq.add(qMaxY, BooleanClause.Occur.MUST);
    return bq;
  }

  /**
   * Constructs a query to retrieve documents that intersect the input envelope.
   *
   * @return the spatial query
   */
  Query makeIntersects(Rectangle bbox) {

    // the original intersects query does not work for envelopes that cross the date line,
    // switch to a NOT Disjoint query

    // MUST_NOT causes a problem when it's the only clause type within a BooleanQuery,
    // to get round it we add all documents as a SHOULD

    // there must be an envelope, it must not be disjoint
    Query qDisjoint = makeDisjoint(bbox);
    Query qIsNonXDL = this.makeXDL(false);
    Query qIsXDL = this.makeXDL(true);
    Query qHasEnv = this.makeQuery(new Query[]{qIsNonXDL, qIsXDL}, BooleanClause.Occur.SHOULD);
    BooleanQuery qNotDisjoint = new BooleanQuery();
    qNotDisjoint.add(qHasEnv, BooleanClause.Occur.MUST);
    qNotDisjoint.add(qDisjoint, BooleanClause.Occur.MUST_NOT);

    //Query qDisjoint = makeDisjoint();
    //BooleanQuery qNotDisjoint = new BooleanQuery();
    //qNotDisjoint.add(new MatchAllDocsQuery(),BooleanClause.Occur.SHOULD);
    //qNotDisjoint.add(qDisjoint,BooleanClause.Occur.MUST_NOT);
    return qNotDisjoint;
  }

  /**
   * Makes a boolean query based upon a collection of queries and a logical operator.
   *
   * @param queries the query collection
   * @param occur the logical operator
   * @return the query
   */
  BooleanQuery makeQuery(Query[] queries, BooleanClause.Occur occur) {
    BooleanQuery bq = new BooleanQuery();
    for (Query query : queries) {
      bq.add(query, occur);
    }
    return bq;
  }

  /**
   * Constructs a query to retrieve documents are fully within the input envelope.
   *
   * @return the spatial query
   */
  Query makeWithin(Rectangle bbox) {

    // general case
    // docMinX >= queryExtent.getMinX() AND docMinY >= queryExtent.getMinY() AND docMaxX <= queryExtent.getMaxX() AND docMaxY <= queryExtent.getMaxY()

    // Y conditions
    // docMinY >= queryExtent.getMinY() AND docMaxY <= queryExtent.getMaxY()
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, precisionStep, bbox.getMinY(), null, true, false);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, precisionStep, null, bbox.getMaxY(), false, true);
    Query yConditions = this.makeQuery(new Query[]{qMinY, qMaxY}, BooleanClause.Occur.MUST);

    // X conditions
    Query xConditions = null;

    // X Conditions for documents that cross the date line,
    // the left portion of the document must be within the left portion of the query,
    // AND the right portion of the document must be within the right portion of the query
    // docMinXLeft >= queryExtent.getMinX() AND docMaxXLeft <= 180.0
    // AND docMinXRight >= -180.0 AND docMaxXRight <= queryExtent.getMaxX()
    Query qXDLLeft = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMinX(), null, true, false);
    Query qXDLRight = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, bbox.getMaxX(), false, true);
    Query qXDLLeftRight = this.makeQuery(new Query[]{qXDLLeft, qXDLRight}, BooleanClause.Occur.MUST);
    Query qXDL = this.makeXDL(true, qXDLLeftRight);

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // docMinX >= queryExtent.getMinX() AND docMaxX <= queryExtent.getMaxX()
      Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMinX(), null, true, false);
      Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, bbox.getMaxX(), false, true);
      Query qMinMax = this.makeQuery(new Query[]{qMinX, qMaxX}, BooleanClause.Occur.MUST);
      Query qNonXDL = this.makeXDL(false, qMinMax);

      // apply the non-XDL or XDL X conditions
      if ((bbox.getMinX() <= -180.0) && bbox.getMaxX() >= 180.0) {
        xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);
      } else {
        xConditions = qNonXDL;
      }

      // queries that cross the date line
    } else {

      // X Conditions for documents that do not cross the date line

      // the document should be within the left portion of the query
      // docMinX >= queryExtent.getMinX() AND docMaxX <= 180.0
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, bbox.getMinX(), null, true, false);
      Query qMaxXLeft = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, 180.0, false, true);
      Query qLeft = this.makeQuery(new Query[]{qMinXLeft, qMaxXLeft}, BooleanClause.Occur.MUST);

      // the document should be within the right portion of the query
      // docMinX >= -180.0 AND docMaxX <= queryExtent.getMaxX()
      Query qMinXRight = NumericRangeQuery.newDoubleRange(field_minX, precisionStep, -180.0, null, true, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(field_maxX, precisionStep, null, bbox.getMaxX(), false, true);
      Query qRight = this.makeQuery(new Query[]{qMinXRight, qMaxXRight}, BooleanClause.Occur.MUST);

      // either left or right conditions should occur,
      // apply the left and right conditions to documents that do not cross the date line
      Query qLeftRight = this.makeQuery(new Query[]{qLeft, qRight}, BooleanClause.Occur.SHOULD);
      Query qNonXDL = this.makeXDL(false, qLeftRight);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);
    }

    // both X and Y conditions must occur
    return this.makeQuery(new Query[]{xConditions, yConditions}, BooleanClause.Occur.MUST);
  }

  /**
   * Constructs a query to retrieve documents that do or do not cross the date line.
   *
   *
   * @param crossedDateLine <code>true</true> for documents that cross the date line
   * @return the query
   */
  Query makeXDL(boolean crossedDateLine) {
    // The 'T' and 'F' values match solr fields
    return new TermQuery(new Term(field_xdl, crossedDateLine ? "T" : "F"));
  }

  /**
   * Constructs a query to retrieve documents that do or do not cross the date line
   * and match the supplied spatial query.
   *
   * @param crossedDateLine <code>true</true> for documents that cross the date line
   * @param query the spatial query
   * @return the query
   */
  Query makeXDL(boolean crossedDateLine, Query query) {
    BooleanQuery bq = new BooleanQuery();
    bq.add(this.makeXDL(crossedDateLine), BooleanClause.Occur.MUST);
    bq.add(query, BooleanClause.Occur.MUST);
    return bq;
  }
}



