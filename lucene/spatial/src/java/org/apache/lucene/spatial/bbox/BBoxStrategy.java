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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.lucene.spatial.util.DistanceToShapeValueSource;
import org.apache.lucene.util.NumericUtils;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;


/**
 * A SpatialStrategy for indexing and searching Rectangles by storing its
 * coordinates in numeric fields. It supports all {@link SpatialOperation}s and
 * has a custom overlap relevancy. It is based on GeoPortal's <a
 * href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialClauseAdapter.java">SpatialClauseAdapter</a>.
 * <p>
 * <b>Characteristics:</b>
 * <p>
 * <ul>
 * <li>Only indexes Rectangles; just one per field value. Other shapes can be provided
 * and the bounding box will be used.</li>
 * <li>Can query only by a Rectangle. Providing other shapes is an error.</li>
 * <li>Supports most {@link SpatialOperation}s but not Overlaps.</li>
 * <li>Uses the DocValues API for any sorting / relevancy.</li>
 * </ul>
 * <p>
 * <b>Implementation:</b>
 * <p>
 * This uses 4 double fields for minX, maxX, minY, maxY
 * and a boolean to mark a dateline cross. Depending on the particular {@link
 * SpatialOperation}s, there are a variety of range queries to be
 * done.
 * The {@link #makeOverlapRatioValueSource(com.spatial4j.core.shape.Rectangle, double)}
 * works by calculating the query bbox overlap percentage against the indexed
 * shape overlap percentage. The indexed shape's coordinates are retrieved from
 * {@link org.apache.lucene.index.LeafReader#getNumericDocValues}.
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
  protected final String field_bbox;
  protected final String field_minX;
  protected final String field_minY;
  protected final String field_maxX;
  protected final String field_maxY;
  protected final String field_xdl; // crosses dateline

  public BBoxStrategy(SpatialContext ctx, String fieldNamePrefix) {
    super(ctx, fieldNamePrefix);
    field_bbox = fieldNamePrefix;
    field_minX = fieldNamePrefix + SUFFIX_MINX;
    field_maxX = fieldNamePrefix + SUFFIX_MAXX;
    field_minY = fieldNamePrefix + SUFFIX_MINY;
    field_maxY = fieldNamePrefix + SUFFIX_MAXY;
    field_xdl = fieldNamePrefix + SUFFIX_XDL;
  }

  //---------------------------------
  // Indexing
  //---------------------------------

  @Override
  public void addFields(Document doc, Shape shape) {
    addFields(doc, shape.getBoundingBox());
  }

  public void addFields(Document doc, Rectangle bbox) {
    doc.addDouble(field_minX, bbox.getMinX());
    doc.addDouble(field_maxX, bbox.getMaxX());
    doc.addDouble(field_minY, bbox.getMinY());
    doc.addDouble(field_maxY, bbox.getMaxY());
    doc.addAtom(field_xdl, bbox.getCrossesDateLine()?"T":"F");
  }

  public void setDocValuesType(FieldTypes fieldTypes, DocValuesType dvType) {
    fieldTypes.setDocValuesType(field_minX, dvType);
    fieldTypes.setDocValuesType(field_minY, dvType);
    fieldTypes.setDocValuesType(field_maxX, dvType);
    fieldTypes.setDocValuesType(field_maxY, dvType);
    fieldTypes.setDocValuesType(field_xdl, dvType);
  }

  public void setIndexOptions(FieldTypes fieldTypes, IndexOptions indexOptions) {
    fieldTypes.setIndexOptions(field_minX, indexOptions);
    fieldTypes.setIndexOptions(field_maxX, indexOptions);
    fieldTypes.setIndexOptions(field_minY, indexOptions);
    fieldTypes.setIndexOptions(field_maxY, indexOptions);
    fieldTypes.setIndexOptions(field_xdl, indexOptions);
  }

  //---------------------------------
  // Value Source / Relevancy
  //---------------------------------

  /**
   * Provides access to each rectangle per document as a ValueSource in which
   * {@link org.apache.lucene.queries.function.FunctionValues#objectVal(int)} returns a {@link
   * Shape}.
   */ //TODO raise to SpatialStrategy
  public ValueSource makeShapeValueSource() {
    return new BBoxValueSource(this);
  }

  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    //TODO if makeShapeValueSource gets lifted to the top; this could become a generic impl.
    return new DistanceToShapeValueSource(makeShapeValueSource(), queryPoint, multiplier, ctx);
  }

  /** Returns a similarity based on {@link BBoxOverlapRatioValueSource}. This is just a
   * convenience method. */
  public ValueSource makeOverlapRatioValueSource(Rectangle queryBox, double queryTargetProportion) {
    return new BBoxOverlapRatioValueSource(
        makeShapeValueSource(), ctx.isGeo(), queryBox, queryTargetProportion, 0.0);
  }

  //---------------------------------
  // Query / Filter Building
  //---------------------------------

  @Override
  public Filter makeFilter(FieldTypes fieldTypes, SpatialArgs args) {
    return new QueryWrapperFilter(makeSpatialQuery(fieldTypes, args));
  }

  @Override
  public ConstantScoreQuery makeQuery(FieldTypes fieldTypes, SpatialArgs args) {
    return new ConstantScoreQuery(makeSpatialQuery(fieldTypes, args));
  }

//  Utility on SpatialStrategy?
//  public Query makeQueryWithValueSource(SpatialArgs args, ValueSource valueSource) {
//    return new FilteredQuery(new FunctionQuery(valueSource), makeFilter(args));
//  }

  private Query makeSpatialQuery(FieldTypes fieldTypes, SpatialArgs args) {
    Shape shape = args.getShape();
    if (!(shape instanceof Rectangle))
      throw new UnsupportedOperationException("Can only query by Rectangle, not " + shape);

    Rectangle bbox = (Rectangle) shape;
    Query spatial;

    // Useful for understanding Relations:
    // http://edndoc.esri.com/arcsde/9.1/general_topics/understand_spatial_relations.htm
    SpatialOperation op = args.getOperation();

    if( op == SpatialOperation.BBoxIntersects      ) spatial = makeIntersects(fieldTypes, bbox);
    else if( op == SpatialOperation.BBoxWithin     ) spatial = makeWithin(fieldTypes, bbox);
    else if( op == SpatialOperation.Contains       ) spatial = makeContains(fieldTypes, bbox);
    else if( op == SpatialOperation.Intersects     ) spatial = makeIntersects(fieldTypes, bbox);
    else if( op == SpatialOperation.IsEqualTo      ) spatial = makeEquals(fieldTypes, bbox);
    else if( op == SpatialOperation.IsDisjointTo   ) spatial = makeDisjoint(fieldTypes, bbox);
    else if( op == SpatialOperation.IsWithin       ) spatial = makeWithin(fieldTypes, bbox);
    else { //no Overlaps support yet
        throw new UnsupportedSpatialOperation(op);
    }
    return spatial;
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   *
   * @return the spatial query
   */
  Query makeContains(FieldTypes fieldTypes, Rectangle bbox) {

    // general case
    // docMinX <= queryExtent.getMinX() AND docMinY <= queryExtent.getMinY() AND docMaxX >= queryExtent.getMaxX() AND docMaxY >= queryExtent.getMaxY()

    // Y conditions
    // docMinY <= queryExtent.getMinY() AND docMaxY >= queryExtent.getMaxY()
    Query qMinY = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minY, null, false, bbox.getMinY(), true));
    Query qMaxY = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxY, bbox.getMaxY(), true, null, false));
    Query yConditions = this.makeQuery(BooleanClause.Occur.MUST, qMinY, qMaxY);

    // X conditions
    Query xConditions;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // documents that contain the min X and max X of the query envelope,
      // docMinX <= queryExtent.getMinX() AND docMaxX >= queryExtent.getMaxX()
      Query qMinX = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, null, false, bbox.getMinX(), true));
      Query qMaxX = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, bbox.getMaxX(), true, null, false));
      Query qMinMax = this.makeQuery(BooleanClause.Occur.MUST, qMinX, qMaxX);
      Query qNonXDL = this.makeXDL(false, qMinMax);

      if (!ctx.isGeo()) {
        xConditions = qNonXDL;
      } else {
        // X Conditions for documents that cross the date line,
        // the left portion of the document contains the min X of the query
        // OR the right portion of the document contains the max X of the query,
        // docMinXLeft <= queryExtent.getMinX() OR docMaxXRight >= queryExtent.getMaxX()
        Query qXDLLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, null, false, bbox.getMinX(), true));
        Query qXDLRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, bbox.getMaxX(), true, null, false));
        Query qXDLLeftRight = this.makeQuery(BooleanClause.Occur.SHOULD, qXDLLeft, qXDLRight);
        Query qXDL = this.makeXDL(true, qXDLLeftRight);

        Query qEdgeDL = null;
        if (bbox.getMinX() == bbox.getMaxX() && Math.abs(bbox.getMinX()) == 180) {
          double edge = bbox.getMinX() * -1;//opposite dateline edge
          qEdgeDL = makeQuery(BooleanClause.Occur.SHOULD,
              makeNumberTermQuery(fieldTypes, field_minX, edge), makeNumberTermQuery(fieldTypes, field_maxX, edge));
        }

        // apply the non-XDL and XDL conditions
        xConditions = this.makeQuery(BooleanClause.Occur.SHOULD, qNonXDL, qXDL, qEdgeDL);
      }
    } else {
      // queries that cross the date line

      // No need to search for documents that do not cross the date line

      // X Conditions for documents that cross the date line,
      // the left portion of the document contains the min X of the query
      // AND the right portion of the document contains the max X of the query,
      // docMinXLeft <= queryExtent.getMinX() AND docMaxXRight >= queryExtent.getMaxX()
      Query qXDLLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, null, false, bbox.getMinX(), true));
      Query qXDLRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, bbox.getMaxX(), true, null, false));
      Query qXDLLeftRight = this.makeXDL(true, this.makeQuery(BooleanClause.Occur.MUST, qXDLLeft, qXDLRight));

      Query qWorld = makeQuery(BooleanClause.Occur.MUST,
          makeNumberTermQuery(fieldTypes, field_minX, -180), makeNumberTermQuery(fieldTypes, field_maxX, 180));

      xConditions = makeQuery(BooleanClause.Occur.SHOULD, qXDLLeftRight, qWorld);
    }

    // both X and Y conditions must occur
    return this.makeQuery(BooleanClause.Occur.MUST, xConditions, yConditions);
  }

  /**
   * Constructs a query to retrieve documents that are disjoint to the input envelope.
   *
   * @return the spatial query
   */
  Query makeDisjoint(FieldTypes fieldTypes, Rectangle bbox) {

    // general case
    // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX() OR docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()

    // Y conditions
    // docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()
    Query qMinY = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minY, bbox.getMaxY(), false, null, false));
    Query qMaxY = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxY, null, false, bbox.getMinY(), false));
    Query yConditions = this.makeQuery(BooleanClause.Occur.SHOULD, qMinY, qMaxY);

    // X conditions
    Query xConditions;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()
      Query qMinX = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, bbox.getMaxX(), false, null, false));
      if (bbox.getMinX() == -180.0 && ctx.isGeo()) {//touches dateline; -180 == 180
        BooleanQuery bq = new BooleanQuery();
        bq.add(qMinX, BooleanClause.Occur.MUST);
        bq.add(makeNumberTermQuery(fieldTypes, field_maxX, 180.0), BooleanClause.Occur.MUST_NOT);
        qMinX = bq;
      }
      Query qMaxX = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, bbox.getMinX(), false));
      if (bbox.getMaxX() == 180.0 && ctx.isGeo()) {//touches dateline; -180 == 180
        BooleanQuery bq = new BooleanQuery();
        bq.add(qMaxX, BooleanClause.Occur.MUST);
        bq.add(makeNumberTermQuery(fieldTypes, field_minX, -180.0), BooleanClause.Occur.MUST_NOT);
        qMaxX = bq;
      }
      Query qMinMax = this.makeQuery(BooleanClause.Occur.SHOULD, qMinX, qMaxX);
      Query qNonXDL = this.makeXDL(false, qMinMax);

      if (!ctx.isGeo()) {
        xConditions = qNonXDL;
      } else {
        // X Conditions for documents that cross the date line,

        // both the left and right portions of the document must be disjoint to the query
        // (docMinXLeft > queryExtent.getMaxX() OR docMaxXLeft < queryExtent.getMinX()) AND
        // (docMinXRight > queryExtent.getMaxX() OR docMaxXRight < queryExtent.getMinX())
        // where: docMaxXLeft = 180.0, docMinXRight = -180.0
        // (docMaxXLeft  < queryExtent.getMinX()) equates to (180.0  < queryExtent.getMinX()) and is ignored
        // (docMinXRight > queryExtent.getMaxX()) equates to (-180.0 > queryExtent.getMaxX()) and is ignored
        Query qMinXLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, bbox.getMaxX(), false, null, false));
        Query qMaxXRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, bbox.getMinX(), false));
        Query qLeftRight = this.makeQuery(BooleanClause.Occur.MUST, qMinXLeft, qMaxXRight);
        Query qXDL = this.makeXDL(true, qLeftRight);

        // apply the non-XDL and XDL conditions
        xConditions = this.makeQuery(BooleanClause.Occur.SHOULD, qNonXDL, qXDL);
      }
      // queries that cross the date line
    } else {

      // X Conditions for documents that do not cross the date line,
      // the document must be disjoint to both the left and right query portions
      // (docMinX > queryExtent.getMaxX()Left OR docMaxX < queryExtent.getMinX()) AND (docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()Left)
      // where: queryExtent.getMaxX()Left = 180.0, queryExtent.getMinX()Left = -180.0
      Query qMinXLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, 180.0, false, null, false));
      Query qMaxXLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false,bbox.getMinX(), false));
      Query qMinXRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, bbox.getMaxX(), false, null, false));
      Query qMaxXRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, -180.0, false));
      Query qLeft = this.makeQuery(BooleanClause.Occur.SHOULD, qMinXLeft, qMaxXLeft);
      Query qRight = this.makeQuery(BooleanClause.Occur.SHOULD, qMinXRight, qMaxXRight);
      Query qLeftRight = this.makeQuery(BooleanClause.Occur.MUST, qLeft, qRight);

      // No need to search for documents that do not cross the date line

      xConditions = this.makeXDL(false, qLeftRight);
    }

    // either X or Y conditions should occur
    return this.makeQuery(BooleanClause.Occur.SHOULD, xConditions, yConditions);
  }

  /**
   * Constructs a query to retrieve documents that equal the input envelope.
   *
   * @return the spatial query
   */
  Query makeEquals(FieldTypes fieldTypes, Rectangle bbox) {

    // docMinX = queryExtent.getMinX() AND docMinY = queryExtent.getMinY() AND docMaxX = queryExtent.getMaxX() AND docMaxY = queryExtent.getMaxY()
    Query qMinX = makeNumberTermQuery(fieldTypes, field_minX, bbox.getMinX());
    Query qMinY = makeNumberTermQuery(fieldTypes, field_minY, bbox.getMinY());
    Query qMaxX = makeNumberTermQuery(fieldTypes, field_maxX, bbox.getMaxX());
    Query qMaxY = makeNumberTermQuery(fieldTypes, field_maxY, bbox.getMaxY());
    return makeQuery(BooleanClause.Occur.MUST, qMinX, qMinY, qMaxX, qMaxY);
  }

  /**
   * Constructs a query to retrieve documents that intersect the input envelope.
   *
   * @return the spatial query
   */
  Query makeIntersects(FieldTypes fieldTypes, Rectangle bbox) {

    // the original intersects query does not work for envelopes that cross the date line,
    // switch to a NOT Disjoint query

    // MUST_NOT causes a problem when it's the only clause type within a BooleanQuery,
    // to get around it we add all documents as a SHOULD

    // there must be an envelope, it must not be disjoint
    Query qHasEnv;
    if (ctx.isGeo()) {
      Query qIsNonXDL = this.makeXDL(false);
      Query qIsXDL = ctx.isGeo() ? this.makeXDL(true) : null;
      qHasEnv = this.makeQuery(BooleanClause.Occur.SHOULD, qIsNonXDL, qIsXDL);
    } else {
      qHasEnv = this.makeXDL(false);
    }

    BooleanQuery qNotDisjoint = new BooleanQuery();
    qNotDisjoint.add(qHasEnv, BooleanClause.Occur.MUST);
    Query qDisjoint = makeDisjoint(fieldTypes, bbox);
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
   * @param occur the logical operator
   * @param queries the query collection
   * @return the query
   */
  BooleanQuery makeQuery(BooleanClause.Occur occur, Query... queries) {
    BooleanQuery bq = new BooleanQuery();
    for (Query query : queries) {
      if (query != null)
        bq.add(query, occur);
    }
    return bq;
  }

  /**
   * Constructs a query to retrieve documents are fully within the input envelope.
   *
   * @return the spatial query
   */
  Query makeWithin(FieldTypes fieldTypes, Rectangle bbox) {

    // general case
    // docMinX >= queryExtent.getMinX() AND docMinY >= queryExtent.getMinY() AND docMaxX <= queryExtent.getMaxX() AND docMaxY <= queryExtent.getMaxY()

    // Y conditions
    // docMinY >= queryExtent.getMinY() AND docMaxY <= queryExtent.getMaxY()
    Query qMinY = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minY, bbox.getMinY(), true, null, false));
    Query qMaxY = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxY, null, false, bbox.getMaxY(), true));
    Query yConditions = this.makeQuery(BooleanClause.Occur.MUST, qMinY, qMaxY);

    // X conditions
    Query xConditions;

    if (ctx.isGeo() && bbox.getMinX() == -180.0 && bbox.getMaxX() == 180.0) {
      //if query world-wraps, only the y condition matters
      return yConditions;

    } else if (!bbox.getCrossesDateLine()) {
      // queries that do not cross the date line

      // docMinX >= queryExtent.getMinX() AND docMaxX <= queryExtent.getMaxX()
      Query qMinX = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, bbox.getMinX(), true, null, false));
      Query qMaxX = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, bbox.getMaxX(), true));
      Query qMinMax = this.makeQuery(BooleanClause.Occur.MUST, qMinX, qMaxX);

      double edge = 0;//none, otherwise opposite dateline of query
      if (bbox.getMinX() == -180.0)
        edge = 180;
      else if (bbox.getMaxX() == 180.0)
        edge = -180;
      if (edge != 0 && ctx.isGeo()) {
        Query edgeQ = makeQuery(BooleanClause.Occur.MUST,
            makeNumberTermQuery(fieldTypes, field_minX, edge), makeNumberTermQuery(fieldTypes, field_maxX, edge));
        qMinMax = makeQuery(BooleanClause.Occur.SHOULD, qMinMax, edgeQ);
      }

      xConditions = this.makeXDL(false, qMinMax);

      // queries that cross the date line
    } else {

      // X Conditions for documents that do not cross the date line

      // the document should be within the left portion of the query
      // docMinX >= queryExtent.getMinX() AND docMaxX <= 180.0
      Query qMinXLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, bbox.getMinX(), true, null, false));
      Query qMaxXLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, 180.0, true));
      Query qLeft = this.makeQuery(BooleanClause.Occur.MUST, qMinXLeft, qMaxXLeft);

      // the document should be within the right portion of the query
      // docMinX >= -180.0 AND docMaxX <= queryExtent.getMaxX()
      Query qMinXRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, -180.0, true, null, false));
      Query qMaxXRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, bbox.getMaxX(), true));
      Query qRight = this.makeQuery(BooleanClause.Occur.MUST, qMinXRight, qMaxXRight);

      // either left or right conditions should occur,
      // apply the left and right conditions to documents that do not cross the date line
      Query qLeftRight = this.makeQuery(BooleanClause.Occur.SHOULD, qLeft, qRight);
      Query qNonXDL = this.makeXDL(false, qLeftRight);

      // X Conditions for documents that cross the date line,
      // the left portion of the document must be within the left portion of the query,
      // AND the right portion of the document must be within the right portion of the query
      // docMinXLeft >= queryExtent.getMinX() AND docMaxXLeft <= 180.0
      // AND docMinXRight >= -180.0 AND docMaxXRight <= queryExtent.getMaxX()
      Query qXDLLeft = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_minX, bbox.getMinX(), true, null, false));
      Query qXDLRight = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter(field_maxX, null, false, bbox.getMaxX(), true));
      Query qXDLLeftRight = this.makeQuery(BooleanClause.Occur.MUST, qXDLLeft, qXDLRight);
      Query qXDL = this.makeXDL(true, qXDLLeftRight);

      // apply the non-XDL and XDL conditions
      xConditions = this.makeQuery(BooleanClause.Occur.SHOULD, qNonXDL, qXDL);
    }

    // both X and Y conditions must occur
    return this.makeQuery(BooleanClause.Occur.MUST, xConditions, yConditions);
  }

  /**
   * Constructs a query to retrieve documents that do or do not cross the date line.
   *
   * @param crossedDateLine <code>true</true> for documents that cross the date line
   * @return the query
   */
  private Query makeXDL(boolean crossedDateLine) {
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
  private Query makeXDL(boolean crossedDateLine, Query query) {
    if (!ctx.isGeo()) {
      assert !crossedDateLine;
      return query;
    }
    BooleanQuery bq = new BooleanQuery();
    bq.add(this.makeXDL(crossedDateLine), BooleanClause.Occur.MUST);
    bq.add(query, BooleanClause.Occur.MUST);
    return bq;
  }

  private Query makeNumberTermQuery(FieldTypes fieldTypes, String field, double number) {
    return new TermQuery(new Term(field, NumericUtils.doubleToBytes(number)));
  }
}



