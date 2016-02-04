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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.lucene.spatial.util.DistanceToShapeValueSource;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;


/**
 * A SpatialStrategy for indexing and searching Rectangles by storing its
 * coordinates in numeric fields. It supports all {@link SpatialOperation}s and
 * has a custom overlap relevancy. It is based on GeoPortal's <a
 * href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialClauseAdapter.java">SpatialClauseAdapter</a>.
 * <p>
 * <b>Characteristics:</b>
 * <br>
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
 * SpatialOperation}s, there are a variety of {@link NumericRangeQuery}s to be
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

  protected FieldType fieldType;//for the 4 numbers
  protected FieldType xdlFieldType;

  public BBoxStrategy(SpatialContext ctx, String fieldNamePrefix) {
    super(ctx, fieldNamePrefix);
    field_bbox = fieldNamePrefix;
    field_minX = fieldNamePrefix + SUFFIX_MINX;
    field_maxX = fieldNamePrefix + SUFFIX_MAXX;
    field_minY = fieldNamePrefix + SUFFIX_MINY;
    field_maxY = fieldNamePrefix + SUFFIX_MAXY;
    field_xdl = fieldNamePrefix + SUFFIX_XDL;

    FieldType fieldType = new FieldType(DoubleField.TYPE_NOT_STORED);
    fieldType.setNumericPrecisionStep(8);//Solr's default
    fieldType.setDocValuesType(DocValuesType.NUMERIC);
    setFieldType(fieldType);
  }

  private int getPrecisionStep() {
    return fieldType.numericPrecisionStep();
  }

  public FieldType getFieldType() {
    return fieldType;
  }

  /** Used to customize the indexing options of the 4 number fields, and to a lesser degree the XDL field too. Search
   * requires indexed=true, and relevancy requires docValues. If these features aren't needed then disable them.
   * {@link FieldType#freeze()} is called on the argument. */
  public void setFieldType(FieldType fieldType) {
    fieldType.freeze();
    this.fieldType = fieldType;
    //only double's supported right now
    if (fieldType.numericType() != FieldType.NumericType.DOUBLE)
      throw new IllegalArgumentException("BBoxStrategy only supports doubles at this time.");
    //for xdlFieldType, copy some similar options. Don't do docValues since it isn't needed here.
    xdlFieldType = new FieldType(StringField.TYPE_NOT_STORED);
    xdlFieldType.setStored(fieldType.stored());
    xdlFieldType.setIndexOptions(fieldType.indexOptions());
    xdlFieldType.freeze();
  }

  //---------------------------------
  // Indexing
  //---------------------------------

  @Override
  public Field[] createIndexableFields(Shape shape) {
    return createIndexableFields(shape.getBoundingBox());
  }

  public Field[] createIndexableFields(Rectangle bbox) {
    Field[] fields = new Field[5];
    fields[0] = new ComboField(field_minX, bbox.getMinX(), fieldType);
    fields[1] = new ComboField(field_maxX, bbox.getMaxX(), fieldType);
    fields[2] = new ComboField(field_minY, bbox.getMinY(), fieldType);
    fields[3] = new ComboField(field_maxY, bbox.getMaxY(), fieldType);
    fields[4] = new ComboField(field_xdl, bbox.getCrossesDateLine()?"T":"F", xdlFieldType);
    return fields;
  }

  /** Field subclass circumventing Field limitations. This one instance can have any combination of indexed, stored,
   * and docValues.
   */
  private static class ComboField extends Field {
    private ComboField(String name, Object value, FieldType type) {
      super(name, type);//this expert constructor allows us to have a field that has docValues & indexed/stored
      super.fieldsData = value;
    }

    //Is this a hack?  We assume that numericValue() is only called for DocValues purposes.
    @Override
    public Number numericValue() {
      //Numeric DocValues only supports Long,
      final Number number = super.numericValue();
      if (number == null)
        return null;
      if (fieldType().numericType() == FieldType.NumericType.DOUBLE)
        return Double.doubleToLongBits(number.doubleValue());
      if (fieldType().numericType() == FieldType.NumericType.FLOAT)
        return Float.floatToIntBits(number.floatValue());
      return number.longValue();
    }
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
  // Query Building
  //---------------------------------

  //  Utility on SpatialStrategy?
//  public Query makeQueryWithValueSource(SpatialArgs args, ValueSource valueSource) {
//    return new CustomScoreQuery(makeQuery(args), new FunctionQuery(valueSource));
  //or...
//  return new BooleanQuery.Builder()
//      .add(new FunctionQuery(valueSource), BooleanClause.Occur.MUST)//matches everything and provides score
//      .add(filterQuery, BooleanClause.Occur.FILTER)//filters (score isn't used)
//  .build();
//  }

  @Override
  public Query makeQuery(SpatialArgs args) {
    Shape shape = args.getShape();
    if (!(shape instanceof Rectangle))
      throw new UnsupportedOperationException("Can only query by Rectangle, not " + shape);

    Rectangle bbox = (Rectangle) shape;
    Query spatial;

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
    else { //no Overlaps support yet
        throw new UnsupportedSpatialOperation(op);
    }
    return new ConstantScoreQuery(spatial);
  }

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
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, getPrecisionStep(), null, bbox.getMinY(), false, true);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, getPrecisionStep(), bbox.getMaxY(), null, true, false);
    Query yConditions = this.makeQuery(BooleanClause.Occur.MUST, qMinY, qMaxY);

    // X conditions
    Query xConditions;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // documents that contain the min X and max X of the query envelope,
      // docMinX <= queryExtent.getMinX() AND docMaxX >= queryExtent.getMaxX()
      Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), null, bbox.getMinX(), false, true);
      Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), bbox.getMaxX(), null, true, false);
      Query qMinMax = this.makeQuery(BooleanClause.Occur.MUST, qMinX, qMaxX);
      Query qNonXDL = this.makeXDL(false, qMinMax);

      if (!ctx.isGeo()) {
        xConditions = qNonXDL;
      } else {
        // X Conditions for documents that cross the date line,
        // the left portion of the document contains the min X of the query
        // OR the right portion of the document contains the max X of the query,
        // docMinXLeft <= queryExtent.getMinX() OR docMaxXRight >= queryExtent.getMaxX()
        Query qXDLLeft = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), null, bbox.getMinX(), false, true);
        Query qXDLRight = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), bbox.getMaxX(), null, true, false);
        Query qXDLLeftRight = this.makeQuery(BooleanClause.Occur.SHOULD, qXDLLeft, qXDLRight);
        Query qXDL = this.makeXDL(true, qXDLLeftRight);

        Query qEdgeDL = null;
        if (bbox.getMinX() == bbox.getMaxX() && Math.abs(bbox.getMinX()) == 180) {
          double edge = bbox.getMinX() * -1;//opposite dateline edge
          qEdgeDL = makeQuery(BooleanClause.Occur.SHOULD,
              makeNumberTermQuery(field_minX, edge), makeNumberTermQuery(field_maxX, edge));
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
      Query qXDLLeft = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), null, bbox.getMinX(), false, true);
      Query qXDLRight = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), bbox.getMaxX(), null, true, false);
      Query qXDLLeftRight = this.makeXDL(true, this.makeQuery(BooleanClause.Occur.MUST, qXDLLeft, qXDLRight));

      Query qWorld = makeQuery(BooleanClause.Occur.MUST,
          makeNumberTermQuery(field_minX, -180), makeNumberTermQuery(field_maxX, 180));

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
  Query makeDisjoint(Rectangle bbox) {

    // general case
    // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX() OR docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()

    // Y conditions
    // docMinY > queryExtent.getMaxY() OR docMaxY < queryExtent.getMinY()
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, getPrecisionStep(), bbox.getMaxY(), null, false, false);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, getPrecisionStep(), null, bbox.getMinY(), false, false);
    Query yConditions = this.makeQuery(BooleanClause.Occur.SHOULD, qMinY, qMaxY);

    // X conditions
    Query xConditions;

    // queries that do not cross the date line
    if (!bbox.getCrossesDateLine()) {

      // X Conditions for documents that do not cross the date line,
      // docMinX > queryExtent.getMaxX() OR docMaxX < queryExtent.getMinX()
      Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), bbox.getMaxX(), null, false, false);
      if (bbox.getMinX() == -180.0 && ctx.isGeo()) {//touches dateline; -180 == 180
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(qMinX, BooleanClause.Occur.MUST);
        bq.add(makeNumberTermQuery(field_maxX, 180.0), BooleanClause.Occur.MUST_NOT);
        qMinX = bq.build();
      }
      Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, bbox.getMinX(), false, false);
      if (bbox.getMaxX() == 180.0 && ctx.isGeo()) {//touches dateline; -180 == 180
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(qMaxX, BooleanClause.Occur.MUST);
        bq.add(makeNumberTermQuery(field_minX, -180.0), BooleanClause.Occur.MUST_NOT);
        qMaxX = bq.build();
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
        Query qMinXLeft = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), bbox.getMaxX(), null, false, false);
        Query qMaxXRight = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, bbox.getMinX(), false, false);
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
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), 180.0, null, false, false);
      Query qMaxXLeft = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, bbox.getMinX(), false, false);
      Query qMinXRight = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), bbox.getMaxX(), null, false, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, -180.0, false, false);
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
  Query makeEquals(Rectangle bbox) {

    // docMinX = queryExtent.getMinX() AND docMinY = queryExtent.getMinY() AND docMaxX = queryExtent.getMaxX() AND docMaxY = queryExtent.getMaxY()
    Query qMinX = makeNumberTermQuery(field_minX, bbox.getMinX());
    Query qMinY = makeNumberTermQuery(field_minY, bbox.getMinY());
    Query qMaxX = makeNumberTermQuery(field_maxX, bbox.getMaxX());
    Query qMaxY = makeNumberTermQuery(field_maxY, bbox.getMaxY());
    return makeQuery(BooleanClause.Occur.MUST, qMinX, qMinY, qMaxX, qMaxY);
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

    BooleanQuery.Builder qNotDisjoint = new BooleanQuery.Builder();
    qNotDisjoint.add(qHasEnv, BooleanClause.Occur.MUST);
    Query qDisjoint = makeDisjoint(bbox);
    qNotDisjoint.add(qDisjoint, BooleanClause.Occur.MUST_NOT);

    //Query qDisjoint = makeDisjoint();
    //BooleanQuery qNotDisjoint = new BooleanQuery();
    //qNotDisjoint.add(new MatchAllDocsQuery(),BooleanClause.Occur.SHOULD);
    //qNotDisjoint.add(qDisjoint,BooleanClause.Occur.MUST_NOT);
    return qNotDisjoint.build();
  }

  /**
   * Makes a boolean query based upon a collection of queries and a logical operator.
   *
   * @param occur the logical operator
   * @param queries the query collection
   * @return the query
   */
  BooleanQuery makeQuery(BooleanClause.Occur occur, Query... queries) {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Query query : queries) {
      if (query != null)
        bq.add(query, occur);
    }
    return bq.build();
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
    Query qMinY = NumericRangeQuery.newDoubleRange(field_minY, getPrecisionStep(), bbox.getMinY(), null, true, false);
    Query qMaxY = NumericRangeQuery.newDoubleRange(field_maxY, getPrecisionStep(), null, bbox.getMaxY(), false, true);
    Query yConditions = this.makeQuery(BooleanClause.Occur.MUST, qMinY, qMaxY);

    // X conditions
    Query xConditions;

    if (ctx.isGeo() && bbox.getMinX() == -180.0 && bbox.getMaxX() == 180.0) {
      //if query world-wraps, only the y condition matters
      return yConditions;

    } else if (!bbox.getCrossesDateLine()) {
      // queries that do not cross the date line

      // docMinX >= queryExtent.getMinX() AND docMaxX <= queryExtent.getMaxX()
      Query qMinX = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), bbox.getMinX(), null, true, false);
      Query qMaxX = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, bbox.getMaxX(), false, true);
      Query qMinMax = this.makeQuery(BooleanClause.Occur.MUST, qMinX, qMaxX);

      double edge = 0;//none, otherwise opposite dateline of query
      if (bbox.getMinX() == -180.0)
        edge = 180;
      else if (bbox.getMaxX() == 180.0)
        edge = -180;
      if (edge != 0 && ctx.isGeo()) {
        Query edgeQ = makeQuery(BooleanClause.Occur.MUST,
            makeNumberTermQuery(field_minX, edge), makeNumberTermQuery(field_maxX, edge));
        qMinMax = makeQuery(BooleanClause.Occur.SHOULD, qMinMax, edgeQ);
      }

      xConditions = this.makeXDL(false, qMinMax);

      // queries that cross the date line
    } else {

      // X Conditions for documents that do not cross the date line

      // the document should be within the left portion of the query
      // docMinX >= queryExtent.getMinX() AND docMaxX <= 180.0
      Query qMinXLeft = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), bbox.getMinX(), null, true, false);
      Query qMaxXLeft = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, 180.0, false, true);
      Query qLeft = this.makeQuery(BooleanClause.Occur.MUST, qMinXLeft, qMaxXLeft);

      // the document should be within the right portion of the query
      // docMinX >= -180.0 AND docMaxX <= queryExtent.getMaxX()
      Query qMinXRight = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), -180.0, null, true, false);
      Query qMaxXRight = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, bbox.getMaxX(), false, true);
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
      Query qXDLLeft = NumericRangeQuery.newDoubleRange(field_minX, getPrecisionStep(), bbox.getMinX(), null, true, false);
      Query qXDLRight = NumericRangeQuery.newDoubleRange(field_maxX, getPrecisionStep(), null, bbox.getMaxX(), false, true);
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
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(this.makeXDL(crossedDateLine), BooleanClause.Occur.MUST);
    bq.add(query, BooleanClause.Occur.MUST);
    return bq.build();
  }

  private Query makeNumberTermQuery(String field, double number) {
    BytesRefBuilder bytes = new BytesRefBuilder();
    NumericUtils.longToPrefixCodedBytes(NumericUtils.doubleToSortableLong(number), 0, bytes);
    return new TermQuery(new Term(field, bytes.get()));
  }

}



