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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.StorableField;
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
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.util.DistanceUnits;
import org.apache.solr.util.MapListener;
import org.apache.solr.util.SpatialUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Solr FieldTypes based on a Lucene 4 {@link SpatialStrategy}.
 *
 * @lucene.experimental
 */
public abstract class AbstractSpatialFieldType<T extends SpatialStrategy> extends FieldType implements SpatialQueryable {

  /** A local-param with one of "none" (default), "distance", "recipDistance" or supported values in ({@link DistanceUnits#getSupportedUnits()}. */
  public static final String SCORE_PARAM = "score";
  /** A local-param boolean that can be set to false to only return the
   * FunctionQuery (score), and thus not do filtering.
   */
  public static final String FILTER_PARAM = "filter";

  //score param values:
  public static final String DISTANCE = "distance";
  public static final String RECIP_DISTANCE = "recipDistance";
  public static final String NONE = "none";

  protected final Logger log = LoggerFactory.getLogger( getClass() );

  protected SpatialContext ctx;
  protected SpatialArgsParser argsParser;

  private final Cache<String, T> fieldStrategyCache = CacheBuilder.newBuilder().build();

  protected DistanceUnits distanceUnits;
  @Deprecated
  protected String units; // for back compat; hopefully null

  protected final Set<String> supportedScoreModes;

  protected AbstractSpatialFieldType() {
    this(Collections.emptySet());
  }

  protected AbstractSpatialFieldType(Set<String> moreScoreModes) {
    Set<String> set = new TreeSet<>();//sorted for consistent display order
    set.add(NONE);
    set.add(DISTANCE);
    set.add(RECIP_DISTANCE);
    set.addAll(DistanceUnits.getSupportedUnits());
    set.addAll(moreScoreModes);
    supportedScoreModes = Collections.unmodifiableSet(set);
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    if(ctx==null) { // subclass can set this directly
      //Solr expects us to remove the parameters we've used.
      MapListener<String, String> argsWrap = new MapListener<>(args);
      ctx = SpatialContextFactory.makeSpatialContext(argsWrap, schema.getResourceLoader().getClassLoader());
      args.keySet().removeAll(argsWrap.getSeenKeys());
    }
    
    final String unitsErrMsg = "units parameter is deprecated, please use distanceUnits instead for field types with class " +
        getClass().getSimpleName();
    this.units = args.remove("units");//deprecated
    if (units != null) {
      if ("degrees".equals(units)) {
        log.warn(unitsErrMsg);
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, unitsErrMsg);
      }
    }

    final String distanceUnitsStr = args.remove("distanceUnits");
    if (distanceUnitsStr == null) {
      if (units != null) {
        this.distanceUnits = DistanceUnits.BACKCOMPAT;
      } else {
        this.distanceUnits = ctx.isGeo() ? DistanceUnits.KILOMETERS : DistanceUnits.DEGREES;
      }
    } else {
      // If both units and distanceUnits was specified
      if (units != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, unitsErrMsg);
      }
      this.distanceUnits = parseDistanceUnits(distanceUnitsStr);
      if (this.distanceUnits == null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Must specify distanceUnits as one of "+ DistanceUnits.getSupportedUnits() +
                " on field types with class "+getClass().getSimpleName());
    }

    argsParser = newSpatialArgsParser();
  }

  /** if {@code str} is non-null, returns {@link org.apache.solr.util.DistanceUnits#valueOf(String)}
   * (which will return null if not found),
   * else returns {@link #distanceUnits} (only null before initialized in {@code init()}.
   * @param str maybe null
   * @return maybe null
   */
  public DistanceUnits parseDistanceUnits(String str) {
    if (str == null) {
      return this.distanceUnits;
    } else {
      return DistanceUnits.valueOf(str);
    }
  }

  protected SpatialArgsParser newSpatialArgsParser() {
    return new SpatialArgsParser() {
      @Override
      protected Shape parseShape(String str, SpatialContext ctx) throws ParseException {
        return AbstractSpatialFieldType.this.parseShape(str);
      }
    };
  }

  //--------------------------------------------------------------
  // Indexing
  //--------------------------------------------------------------

  @Override
  public final Field createField(SchemaField field, Object val, float boost) {
    throw new IllegalStateException("instead call createFields() because isPolyField() is true");
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public List<StorableField> createFields(SchemaField field, Object val, float boost) {
    String shapeStr = null;
    Shape shape;
    if (val instanceof Shape) {
      shape = ((Shape) val);
    } else {
      shapeStr = val.toString();
      shape = parseShape(shapeStr);
    }
    if (shape == null) {
      log.debug("Field {}: null shape for input: {}", field, val);
      return Collections.emptyList();
    }

    List<StorableField> result = new ArrayList<>();
    if (field.indexed()) {
      T strategy = getStrategy(field.getName());
      result.addAll(Arrays.asList(strategy.createIndexableFields(shape)));
    }

    if (field.stored()) {
      result.add(new StoredField(field.getName(), getStoredValue(shape, shapeStr)));
    }

    return result;
  }

  /** Called by {@link #createFields(SchemaField, Object, float)} to get the stored value. */
  protected String getStoredValue(Shape shape, String shapeStr) {
    return (shapeStr == null) ? shapeToString(shape) : shapeStr;
  }

  protected Shape parseShape(String str) {
    if (str.length() == 0)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "empty string shape");
    if (Character.isLetter(str.charAt(0))) {//WKT starts with a letter
      try {
        return ctx.readShapeFromWkt(str);
      } catch (Exception e) {
        String message = e.getMessage();
        if (!message.contains(str))
          message = "Couldn't parse shape '" + str + "' because: " + message;
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message, e);
      }
    } else {
      return SpatialUtils.parsePointSolrException(str, ctx);
    }
  }

  /**
   * Returns a String version of a shape to be used for the stored value. This method in Solr is only called if for some
   * reason a Shape object is passed to the field type (perhaps via a custom UpdateRequestProcessor),
   * *and* the field is marked as stored.  <em>The default implementation throws an exception.</em>
   * <p>
   * Spatial4j 0.4 is probably the last release to support SpatialContext.toString(shape) but it's deprecated with no
   * planned replacement.  Shapes do have a toString() method but they are generally internal/diagnostic and not
   * standard WKT.
   * The solution is subclassing and calling ctx.toString(shape) or directly using LegacyShapeReadWriterFormat or
   * passing in some sort of custom wrapped shape that holds a reference to a String or can generate it.
   */
  protected String shapeToString(Shape shape) {
//    return ctx.toString(shape);
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "Getting a String from a Shape is no longer possible. See javadocs for commentary.");
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

  /**
   * Implemented for compatibility with geofilt &amp; bbox query parsers:
   * {@link SpatialQueryable}.
   */
  @Override
  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    Point pt = SpatialUtils.parsePointSolrException(options.pointStr, ctx);

    double distDeg = DistanceUtils.dist2Degrees(options.distance, options.radius);

    Shape shape = ctx.makeCircle(pt, distDeg);
    if (options.bbox)
      shape = shape.getBoundingBox();

    SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects, shape);
    return getQueryFromSpatialArgs(parser, options.field, spatialArgs);
  }

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    if (!minInclusive || !maxInclusive)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Both sides of spatial range query must be inclusive: " + field.getName());
    Point p1 = SpatialUtils.parsePointSolrException(part1, ctx);
    Point p2 = SpatialUtils.parsePointSolrException(part2, ctx);

    Rectangle bbox = ctx.makeRectangle(p1, p2);
    SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects, bbox);
    return getQueryFromSpatialArgs(parser, field, spatialArgs);//won't score by default
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    //This is different from Solr 3 LatLonType's approach which uses the MultiValueSource concept to directly expose
    // the x & y pair of FieldCache value sources.
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "A ValueSource isn't directly available from this field. Instead try a query using the distance as the score.");
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return getQueryFromSpatialArgs(parser, field, parseSpatialArgs(parser, externalVal));
  }

  protected SpatialArgs parseSpatialArgs(QParser parser, String externalVal) {
    try {
      SpatialArgs args = argsParser.parse(externalVal, ctx);
      // Convert parsed args.distErr to degrees (using distanceUnits)
      if (args.getDistErr() != null) {
        args.setDistErr(args.getDistErr() * distanceUnits.multiplierFromThisUnitToDegrees());
      }
      return args;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  protected Query getQueryFromSpatialArgs(QParser parser, SchemaField field, SpatialArgs spatialArgs) {
    T strategy = getStrategy(field.getName());

    SolrParams localParams = parser.getLocalParams();
    String scoreParam = (localParams == null ? null : localParams.get(SCORE_PARAM));

    //We get the valueSource for the score then the filter and combine them.

    ValueSource valueSource = getValueSourceFromSpatialArgs(parser, field, spatialArgs, scoreParam, strategy);
    if (valueSource == null) {
      //FYI Solr FieldType doesn't have a getFilter(). We'll always grab
      // getQuery() but it's possible a strategy has a more efficient getFilter
      // that could be wrapped -- no way to know.
      //See SOLR-2883 needScore
      return strategy.makeQuery(spatialArgs); //ConstantScoreQuery
    }

    FunctionQuery functionQuery = new FunctionQuery(valueSource);

    if (localParams != null && !localParams.getBool(FILTER_PARAM, true))
      return functionQuery;

    Filter filter = strategy.makeFilter(spatialArgs);
    return new FilteredQuery(functionQuery, filter);
  }

  @Override
  public double getSphereRadius() {
      return distanceUnits.getEarthRadius();
  }

  /** The set of values supported for the score local-param. Not null. */
  public Set<String> getSupportedScoreModes() {
    return supportedScoreModes;
  }

  protected ValueSource getValueSourceFromSpatialArgs(QParser parser, SchemaField field, SpatialArgs spatialArgs, String score, T strategy) {
    if (score == null) {
      return null;
    }

    final double multiplier; // default multiplier for degrees

    switch(score) {
      case "":
      case NONE:
        return null;
      case RECIP_DISTANCE:
        return strategy.makeRecipDistanceValueSource(spatialArgs.getShape());
      case DISTANCE:
        multiplier = distanceUnits.multiplierFromDegreesToThisUnit();
        break;
      default:
        DistanceUnits du = parseDistanceUnits(score);
        if (du != null) {
          multiplier = du.multiplierFromDegreesToThisUnit();
        } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "'score' local-param must be one of " + supportedScoreModes + ", it was: " + score);
    }
  }

    return strategy.makeDistanceValueSource(spatialArgs.getShape().getCenter(), multiplier);
  }

  /**
   * Gets the cached strategy for this field, creating it if necessary
   * via {@link #newSpatialStrategy(String)}.
   * @param fieldName Mandatory reference to the field name
   * @return Non-null.
   */
  public T getStrategy(final String fieldName) {
    try {
      return fieldStrategyCache.get(fieldName, new Callable<T>() {
        @Override
        public T call() throws Exception {
          return newSpatialStrategy(fieldName);
        }
      });
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on SpatialField: " + field.getName()+
      ", instead try sorting by query.");
  }

  public DistanceUnits getDistanceUnits() {
    return this.distanceUnits;
  }
}


