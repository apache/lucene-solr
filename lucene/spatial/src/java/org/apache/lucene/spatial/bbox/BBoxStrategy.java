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
package org.apache.lucene.spatial.bbox;

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
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
import org.apache.lucene.spatial.util.NumericFieldInfo;

import com.spatial4j.core.context.*;
import com.spatial4j.core.exception.UnsupportedSpatialOperation;
import com.spatial4j.core.query.*;
import com.spatial4j.core.shape.*;


/**
 * Based on GeoPortal's
 * <a href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialClauseAdapter.java">SpatialClauseAdapter</a>.
 *
 * @lucene.experimental
 */
public class BBoxStrategy extends SpatialStrategy<BBoxFieldInfo> {
  public double queryPower = 1.0;
  public double targetPower = 1.0f;

  public NumericFieldInfo finfo = null;

  public BBoxStrategy(SpatialContext ctx) {
    super(ctx);
  }

  //---------------------------------
  // Indexing
  //---------------------------------

  @Override
  public IndexableField[] createFields(BBoxFieldInfo fieldInfo,
      Shape shape, boolean index, boolean store) {

    Rectangle bbox = shape.getBoundingBox();
    IndexableField[] fields = new IndexableField[store?6:5];
    fields[0] = finfo.createDouble(fieldInfo.minX, bbox.getMinX());
    fields[1] = finfo.createDouble(fieldInfo.maxX, bbox.getMaxX());
    fields[2] = finfo.createDouble(fieldInfo.minY, bbox.getMinY());
    fields[3] = finfo.createDouble(fieldInfo.maxY, bbox.getMaxY());

    FieldType ft = new FieldType();
    ft.setIndexed(index);
    ft.setStored(store);
    ft.setTokenized(false);
    ft.setOmitNorms(true);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    ft.freeze();

    Field xdl = new Field( fieldInfo.xdl, bbox.getCrossesDateLine()?"T":"F", ft );
    fields[4] = xdl;
    if( store ) {
      FieldType ff = new FieldType();
      ff.setIndexed(false);
      ff.setStored(true);
      ff.setOmitNorms(true);
      ff.setIndexOptions(IndexOptions.DOCS_ONLY);
      ff.freeze();

      NumberFormat nf = NumberFormat.getInstance( Locale.US );
      nf.setMaximumFractionDigits( 5 );
      nf.setMinimumFractionDigits( 5 );
      nf.setGroupingUsed(false);
      String ext =
        nf.format( bbox.getMinX() ) + ' ' +
        nf.format( bbox.getMinY() ) + ' ' +
        nf.format( bbox.getMaxX() ) + ' ' +
        nf.format( bbox.getMaxY() ) + ' ';
      fields[5] = new Field( fieldInfo.bbox, ext, ff );
    }
    return fields;
  }

  @Override
  public IndexableField createField(BBoxFieldInfo fieldInfo, Shape shape,
      boolean index, boolean store) {
    throw new UnsupportedOperationException("BBOX is poly field");
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  //---------------------------------
  // Query Builder
  //---------------------------------

  @Override
  public ValueSource makeValueSource(SpatialArgs args, BBoxFieldInfo fields) {
    return new BBoxSimilarityValueSource(
        new AreaSimilarity(args.getShape().getBoundingBox(), queryPower, targetPower), fields );
  }


  @Override
  public Filter makeFilter(SpatialArgs args, BBoxFieldInfo fieldInfo) {
    Query spatial = makeSpatialQuery(args, fieldInfo);
    return new QueryWrapperFilter( spatial );
  }

  @Override
  public Query makeQuery(SpatialArgs args, BBoxFieldInfo fieldInfo) {
    BooleanQuery bq = new BooleanQuery();
    Query spatial = makeSpatialQuery(args, fieldInfo);
    bq.add(new ConstantScoreQuery(spatial), BooleanClause.Occur.MUST);
    
    // This part does the scoring
    Query spatialRankingQuery = new FunctionQuery(makeValueSource(args, fieldInfo));
    bq.add(spatialRankingQuery, BooleanClause.Occur.MUST);
    return bq;
  }


  private Query makeSpatialQuery(SpatialArgs args, BBoxFieldInfo fieldInfo) {
    Rectangle bbox = args.getShape().getBoundingBox();
    Query spatial = null;

    // Useful for understanding Relations:
    // http://edndoc.esri.com/arcsde/9.1/general_topics/understand_spatial_relations.htm
    SpatialOperation op = args.getOperation();
         if( op == SpatialOperation.BBoxIntersects ) spatial = makeIntersects(bbox, fieldInfo);
    else if( op == SpatialOperation.BBoxWithin     ) spatial = makeWithin(bbox, fieldInfo);
    else if( op == SpatialOperation.Contains       ) spatial = makeContains(bbox, fieldInfo);
    else if( op == SpatialOperation.Intersects     ) spatial = makeIntersects(bbox, fieldInfo);
    else if( op == SpatialOperation.IsEqualTo      ) spatial = makeEquals(bbox, fieldInfo);
    else if( op == SpatialOperation.IsDisjointTo   ) spatial = makeDisjoint(bbox, fieldInfo);
    else if( op == SpatialOperation.IsWithin       ) spatial = makeWithin(bbox, fieldInfo);
    else if( op == SpatialOperation.Overlaps       ) spatial = makeIntersects(bbox, fieldInfo);
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
  Query makeContains(Rectangle bbox, BBoxFieldInfo fieldInfo) {

    // general case
    // docMinX <= queryExtent.getMinX() AND docMinY <= queryExtent.getMinY() AND docMaxX >= queryExtent.getMaxX() AND docMaxY >= queryExtent.getMaxY()

    // Y conditions
    // docMinY <= queryExtent.getMinY() AND docMaxY >= queryExtent.getMaxY()
    Query qMinY = NumericRangeQuery.newDoubleRange(fieldInfo.minY, finfo.precisionStep, null, bbox.getMinY(), false, true);
    Query qMaxY = NumericRangeQuery.newDoubleRange(fieldInfo.maxY, finfo.precisionStep, bbox.getMaxY(), null, true, false);
    Query yConditions = this.makeQuery(new Query[]{qMinY, qMaxY}, BooleanClause.Occur.MUST);

    // X conditions
    Query xConditions = null;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // documents that contain the min X and max X of the query envelope,
      // docMinX <= queryExtent.getMinX() AND docMaxX >= queryExtent.getMaxX()
      Query qMinX = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, null, bbox.getMinX(), false, true);
      Query qMaxX = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, bbox.getMaxX(), null, true, false);
      Query qMinMax = this.makeQuery(new Query[]{qMinX, qMaxX}, BooleanClause.Occur.MUST);
      Query qNonXDL = this.makeXDL(false, qMinMax, fieldInfo);

      // X Conditions for documents that cross the date line,
      // the left portion of the document contains the min X of the query
      // OR the right portion of the document contains the max X of the query,
      // docMinXLeft <= queryExtent.getMinX() OR docMaxXRight >= queryExtent.getMaxX()
      Query qXDLLeft = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, null, bbox.getMinX(), false, true);
      Query qXDLRight = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, bbox.getMaxX(), null, true, false);
      Query qXDLLeftRight = this.makeQuery(new Query[]{qXDLLeft, qXDLRight}, BooleanClause.Occur.SHOULD);
      Query qXDL = this.makeXDL(true, qXDLLeftRight, fieldInfo);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);

      // queries that cross the date line
    } else {

      // No need to search for documents that do not cross the date line

      // X Conditions for documents that cross the date line,
      // the left portion of the document contains the min X of the query
      // AND the right portion of the document contains the max X of the query,
      // docMinXLeft <= queryExtent.getMinX() AND docMaxXRight >= queryExtent.getMaxX()
      Query qXDLLeft = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, null, bbox.getMinX(), false, true);
      Query qXDLRight = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, bbox.getMaxX(), null, true, false);
      Query qXDLLeftRight = this.makeQuery(new Query[]{qXDLLeft, qXDLRight}, BooleanClause.Occur.MUST);

      xConditions = this.makeXDL(true, qXDLLeftRight, fieldInfo);
    }

    // both X and Y conditions must occur
    return this.makeQuery(new Query[]{xConditions, yConditions}, BooleanClause.Occur.MUST);
  }

  /**
   * Constructs a query to retrieve documents that are disjoint to the input envelope.
   *
   * @return the spatial query
   */
  Query makeDisjoint(Rectangle bbox, BBoxFieldInfo fieldInfo) {

    // general case
    // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX() OR docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()

    // Y conditions
    // docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()
    Query qMinY = NumericRangeQuery.newDoubleRange(fieldInfo.minY, finfo.precisionStep, bbox.getMaxY(), null, false, false);
    Query qMaxY = NumericRangeQuery.newDoubleRange(fieldInfo.maxY, finfo.precisionStep, null, bbox.getMinY(), false, false);
    Query yConditions = this.makeQuery(new Query[]{qMinY, qMaxY}, BooleanClause.Occur.SHOULD);

    // X conditions
    Query xConditions = null;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()
      Query qMinX = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMaxX(), null, false, false);
      Query qMaxX = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, bbox.getMinX(), false, false);
      Query qMinMax = this.makeQuery(new Query[]{qMinX, qMaxX}, BooleanClause.Occur.SHOULD);
      Query qNonXDL = this.makeXDL(false, qMinMax, fieldInfo);

      // X Conditions for documents that cross the date line,
      // both the left and right portions of the document must be disjoint to the query
      // (docMinXLeft > queryExtent.getMaxX() OR docMaxXLeft < queryExtent.getMinX()) AND
      // (docMinXRight > queryExtent.getMaxX() OR docMaxXRight < queryExtent.getMinX())
      // where: docMaxXLeft = 180.0, docMinXRight = -180.0
      // (docMaxXLeft  < queryExtent.getMinX()) equates to (180.0  < queryExtent.getMinX()) and is ignored
      // (docMinXRight > queryExtent.getMaxX()) equates to (-180.0 > queryExtent.getMaxX()) and is ignored
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMaxX(), null, false, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, bbox.getMinX(), false, false);
      Query qLeftRight = this.makeQuery(new Query[]{qMinXLeft, qMaxXRight}, BooleanClause.Occur.MUST);
      Query qXDL = this.makeXDL(true, qLeftRight, fieldInfo);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);

      // queries that cross the date line
    } else {

      // X Conditions for documents that do not cross the date line,
      // the document must be disjoint to both the left and right query portions
      // (docMinX > queryExtent.getMaxX()Left OR docMaxX < queryExtent.getMinX()) AND (docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()Left)
      // where: queryExtent.getMaxX()Left = 180.0, queryExtent.getMinX()Left = -180.0
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, 180.0, null, false, false);
      Query qMaxXLeft = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, bbox.getMinX(), false, false);
      Query qMinXRight = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMaxX(), null, false, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, -180.0, false, false);
      Query qLeft = this.makeQuery(new Query[]{qMinXLeft, qMaxXLeft}, BooleanClause.Occur.SHOULD);
      Query qRight = this.makeQuery(new Query[]{qMinXRight, qMaxXRight}, BooleanClause.Occur.SHOULD);
      Query qLeftRight = this.makeQuery(new Query[]{qLeft, qRight}, BooleanClause.Occur.MUST);

      // No need to search for documents that do not cross the date line

      xConditions = this.makeXDL(false, qLeftRight, fieldInfo);
    }

    // either X or Y conditions should occur
    return this.makeQuery(new Query[]{xConditions, yConditions}, BooleanClause.Occur.SHOULD);
  }

  /**
   * Constructs a query to retrieve documents that equal the input envelope.
   *
   * @return the spatial query
   */
  Query makeEquals(Rectangle bbox, BBoxFieldInfo fieldInfo) {

    // docMinX = queryExtent.getMinX() AND docMinY = queryExtent.getMinY() AND docMaxX = queryExtent.getMaxX() AND docMaxY = queryExtent.getMaxY()
    Query qMinX = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMinX(), bbox.getMinX(), true, true);
    Query qMinY = NumericRangeQuery.newDoubleRange(fieldInfo.minY, finfo.precisionStep, bbox.getMinY(), bbox.getMinY(), true, true);
    Query qMaxX = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, bbox.getMaxX(), bbox.getMaxX(), true, true);
    Query qMaxY = NumericRangeQuery.newDoubleRange(fieldInfo.maxY, finfo.precisionStep, bbox.getMaxY(), bbox.getMaxY(), true, true);
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
  Query makeIntersects(Rectangle bbox, BBoxFieldInfo fieldInfo) {

    // the original intersects query does not work for envelopes that cross the date line,
    // switch to a NOT Disjoint query

    // MUST_NOT causes a problem when it's the only clause type within a BooleanQuery,
    // to get round it we add all documents as a SHOULD

    // there must be an envelope, it must not be disjoint
    Query qDisjoint = makeDisjoint(bbox, fieldInfo);
    Query qIsNonXDL = this.makeXDL(false, fieldInfo);
    Query qIsXDL = this.makeXDL(true, fieldInfo);
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
  Query makeWithin(Rectangle bbox, BBoxFieldInfo fieldInfo) {

    // general case
    // docMinX >= queryExtent.getMinX() AND docMinY >= queryExtent.getMinY() AND docMaxX <= queryExtent.getMaxX() AND docMaxY <= queryExtent.getMaxY()

    // Y conditions
    // docMinY >= queryExtent.getMinY() AND docMaxY <= queryExtent.getMaxY()
    Query qMinY = NumericRangeQuery.newDoubleRange(fieldInfo.minY, finfo.precisionStep, bbox.getMinY(), null, true, false);
    Query qMaxY = NumericRangeQuery.newDoubleRange(fieldInfo.maxY, finfo.precisionStep, null, bbox.getMaxY(), false, true);
    Query yConditions = this.makeQuery(new Query[]{qMinY, qMaxY}, BooleanClause.Occur.MUST);

    // X conditions
    Query xConditions = null;

    // X Conditions for documents that cross the date line,
    // the left portion of the document must be within the left portion of the query,
    // AND the right portion of the document must be within the right portion of the query
    // docMinXLeft >= queryExtent.getMinX() AND docMaxXLeft <= 180.0
    // AND docMinXRight >= -180.0 AND docMaxXRight <= queryExtent.getMaxX()
    Query qXDLLeft = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMinX(), null, true, false);
    Query qXDLRight = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, bbox.getMaxX(), false, true);
    Query qXDLLeftRight = this.makeQuery(new Query[]{qXDLLeft, qXDLRight}, BooleanClause.Occur.MUST);
    Query qXDL = this.makeXDL(true, qXDLLeftRight, fieldInfo);

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // docMinX >= queryExtent.getMinX() AND docMaxX <= queryExtent.getMaxX()
      Query qMinX = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMinX(), null, true, false);
      Query qMaxX = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, bbox.getMaxX(), false, true);
      Query qMinMax = this.makeQuery(new Query[]{qMinX, qMaxX}, BooleanClause.Occur.MUST);
      Query qNonXDL = this.makeXDL(false, qMinMax, fieldInfo);

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
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, bbox.getMinX(), null, true, false);
      Query qMaxXLeft = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, 180.0, false, true);
      Query qLeft = this.makeQuery(new Query[]{qMinXLeft, qMaxXLeft}, BooleanClause.Occur.MUST);

      // the document should be within the right portion of the query
      // docMinX >= -180.0 AND docMaxX <= queryExtent.getMaxX()
      Query qMinXRight = NumericRangeQuery.newDoubleRange(fieldInfo.minX, finfo.precisionStep, -180.0, null, true, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(fieldInfo.maxX, finfo.precisionStep, null, bbox.getMaxX(), false, true);
      Query qRight = this.makeQuery(new Query[]{qMinXRight, qMaxXRight}, BooleanClause.Occur.MUST);

      // either left or right conditions should occur,
      // apply the left and right conditions to documents that do not cross the date line
      Query qLeftRight = this.makeQuery(new Query[]{qLeft, qRight}, BooleanClause.Occur.SHOULD);
      Query qNonXDL = this.makeXDL(false, qLeftRight, fieldInfo);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(new Query[]{qNonXDL, qXDL}, BooleanClause.Occur.SHOULD);
    }

    // both X and Y conditions must occur
    return this.makeQuery(new Query[]{xConditions, yConditions}, BooleanClause.Occur.MUST);
  }

  /**
   * Constructs a query to retrieve documents that do or do not cross the date line.
   *
   * @param crossedDateLine <code>true</true> for documents that cross the date line
   * @return the query
   */
  Query makeXDL(boolean crossedDateLine, BBoxFieldInfo fieldInfo) {
    // The 'T' and 'F' values match solr fields
    return new TermQuery(new Term(fieldInfo.xdl, crossedDateLine ? "T" : "F"));
  }

  /**
   * Constructs a query to retrieve documents that do or do not cross the date line
   * and match the supplied spatial query.
   *
   * @param crossedDateLine <code>true</true> for documents that cross the date line
   * @param query the spatial query
   * @return the query
   */
  Query makeXDL(boolean crossedDateLine, Query query, BBoxFieldInfo fieldInfo) {
    BooleanQuery bq = new BooleanQuery();
    bq.add(this.makeXDL(crossedDateLine, fieldInfo), BooleanClause.Occur.MUST);
    bq.add(query, BooleanClause.Occur.MUST);
    return bq;
  }
}



