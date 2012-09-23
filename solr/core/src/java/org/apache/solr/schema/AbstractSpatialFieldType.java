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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.util.MapListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class for Solr FieldTypes based on a Lucene 4 {@link SpatialStrategy}.
 *
 * @lucene.experimental
 */
public abstract class AbstractSpatialFieldType<T extends SpatialStrategy> extends FieldType {

  /** A local-param with one of "none" (default), "distance", or "recipDistance". */
  public static final String SCORE_PARAM = "score";
  protected final Logger log = LoggerFactory.getLogger( getClass() );

  protected SpatialContext ctx;
  protected SpatialArgsParser argsParser;

  private final ConcurrentHashMap<String, T> fieldStrategyMap = new ConcurrentHashMap<String,T>();

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    String units = args.remove("units");
    if (!"degrees".equals(units))
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Must specify units=\"degrees\" on field types with class "+getClass().getSimpleName());

    //Solr expects us to remove the parameters we've used.
    MapListener<String, String> argsWrap = new MapListener<String, String>(args);
    ctx = SpatialContextFactory.makeSpatialContext(argsWrap, schema.getResourceLoader().getClassLoader());
    args.keySet().removeAll(argsWrap.getSeenKeys());

    argsParser = new SpatialArgsParser();//might make pluggable some day?
  }

  //--------------------------------------------------------------
  // Indexing
  //--------------------------------------------------------------

  @Override
  public final Field createField(SchemaField field, Object val, float boost) {
    throw new IllegalStateException("should be calling createFields because isPolyField() is true");
  }

  @Override
  public final Field[] createFields(SchemaField field, Object val, float boost) {
    String shapeStr = null;
    Shape shape = null;
    if (val instanceof Shape) {
      shape = ((Shape) val);
    } else {
      shapeStr = val.toString();
      shape = ctx.readShape(shapeStr);
    }
    if( shape == null ) {
      log.debug("Field {}: null shape for input: {}", field, val);
      return null;
    }

    Field[] indexableFields = null;
    if (field.indexed()) {
      T strategy = getStrategy(field.getName());
      indexableFields = strategy.createIndexableFields(shape);
    }

    StoredField storedField = null;
    if (field.stored()) {
      if (shapeStr == null)
        shapeStr = shapeToString(shape);
      storedField = new StoredField(field.getName(), shapeStr);
    }

    if (indexableFields == null) {
      if (storedField == null)
        return null;
      return new Field[]{storedField};
    } else {
      if (storedField == null)
        return indexableFields;
      Field[] result = new Field[indexableFields.length+1];
      System.arraycopy(indexableFields,0,result,0,indexableFields.length);
      result[result.length-1] = storedField;
      return result;
    }
  }

  protected String shapeToString(Shape shape) {
    return ctx.toString(shape);
  }

  /** Called from {@link #getStrategy(String)} upon first use by fieldName. } */
  protected abstract T newSpatialStrategy(String fieldName);

  @Override
  public final boolean isPolyField() {
    return true;
  }

  //--------------------------------------------------------------
  // Query Support
  //--------------------------------------------------------------

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    if (!minInclusive || !maxInclusive)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Both sides of spatial range query must be inclusive: " + field.getName());
    Shape shape1 = ctx.readShape(part1);
    Shape shape2 = ctx.readShape(part2);
    if (!(shape1 instanceof Point) || !(shape2 instanceof Point))
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Both sides of spatial range query must be points: " + field.getName());
    Point p1 = (Point) shape1;
    Point p2 = (Point) shape2;
    Rectangle bbox = ctx.makeRectangle(p1, p2);
    SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects, bbox);
    return getQueryFromSpatialArgs(parser, field, spatialArgs);//won't score by default
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    //This is different from Solr 3 LatLonType's approach which uses the MultiValueSource concept to directly expose
    // the an x & y pair of FieldCache value sources.
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "A ValueSource isn't directly available from this field. Instead try a query using the distance as the score.");
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return getQueryFromSpatialArgs(parser, field, argsParser.parse(externalVal, ctx));
  }

  private Query getQueryFromSpatialArgs(QParser parser, SchemaField field, SpatialArgs spatialArgs) {
    T strategy = getStrategy(field.getName());

    SolrParams localParams = parser.getLocalParams();
    String score = (localParams == null ? null : localParams.get(SCORE_PARAM));
    if (score == null || "none".equals(score) || "".equals(score)) {
      //FYI Solr FieldType doesn't have a getFilter(). We'll always grab
      // getQuery() but it's possible a strategy has a more efficient getFilter
      // that could be wrapped -- no way to know.
      //See SOLR-2883 needScore
      return strategy.makeQuery(spatialArgs); //ConstantScoreQuery
    }

    //We get the valueSource for the score then the filter and combine them.
    ValueSource valueSource;
    if ("distance".equals(score))
      valueSource = strategy.makeDistanceValueSource(spatialArgs.getShape().getCenter());
    else if ("recipDistance".equals(score))
      valueSource = strategy.makeRecipDistanceValueSource(spatialArgs.getShape());
    else
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'score' local-param must be one of 'none', 'distance', or 'recipDistance'");

    Filter filter = strategy.makeFilter(spatialArgs);
    return new FilteredQuery(new FunctionQuery(valueSource), filter);
  }

  /**
   * Gets the cached strategy for this field, creating it if necessary
   * via {@link #newSpatialStrategy(String)}.
   * @param fieldName Mandatory reference to the field name
   * @return Non-null.
   */
  public T getStrategy(final String fieldName) {
    T strategy = fieldStrategyMap.get(fieldName);
    //double-checked locking idiom
    if (strategy == null) {
      synchronized (fieldStrategyMap) {
        strategy = fieldStrategyMap.get(fieldName);
        if (strategy == null) {
          strategy = newSpatialStrategy(fieldName);
          fieldStrategyMap.put(fieldName,strategy);
        }
      }
    }
    return strategy;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on SpatialField: " + field.getName());
  }
}


