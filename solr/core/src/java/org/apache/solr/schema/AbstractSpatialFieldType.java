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
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.spatial4j.Geo3dSpatialContextFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.util.DistanceUnits;
import org.apache.solr.util.MapListener;
import org.apache.solr.util.SpatialUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.SupportedFormats;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
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

  /** Optional param to pick the string conversion */
  public static final String FORMAT = "format";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected SpatialContext ctx;
  protected SpatialArgsParser argsParser;

  protected ShapeWriter shapeWriter;
  protected ShapeReader shapeReader;

  private final Cache<String, T> fieldStrategyCache = CacheBuilder.newBuilder().build();

  protected DistanceUnits distanceUnits;

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

    if (ctx==null) { // subclass can set this directly
      final String CTX_PARAM = "spatialContextFactory";
      final String OLD_SPATIAL4J_PREFIX = "com.spatial4j.core";
      final String NEW_SPATIAL4J_PREFIX = "org.locationtech.spatial4j";
      for (Map.Entry<String, String> argEntry : args.entrySet()) {
        // "JTS" is a convenience alias
        if (argEntry.getKey().equals(CTX_PARAM) && argEntry.getValue().equals("JTS")) {
          argEntry.setValue("org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory");
          continue;
        }
        if (argEntry.getKey().equals(CTX_PARAM) && argEntry.getValue().equals("Geo3D")) {
          argEntry.setValue(Geo3dSpatialContextFactory.class.getName());
          continue;
        }
        // Warn about using old Spatial4j class names
        if (argEntry.getValue().contains(OLD_SPATIAL4J_PREFIX)) {
          log.warn("Replace '{}' with '{}' in your schema", OLD_SPATIAL4J_PREFIX, NEW_SPATIAL4J_PREFIX);
          argEntry.setValue(argEntry.getValue().replace(OLD_SPATIAL4J_PREFIX, NEW_SPATIAL4J_PREFIX));
        }
      }

      //Solr expects us to remove the parameters we've used.
      MapListener<String, String> argsWrap = new MapListener<>(args);
      ctx = SpatialContextFactory.makeSpatialContext(argsWrap, schema.getResourceLoader().getClassLoader());
      args.keySet().removeAll(argsWrap.getSeenKeys());
    }

    final String distanceUnitsStr = args.remove("distanceUnits");
    if (distanceUnitsStr == null) {
      this.distanceUnits = ctx.isGeo() ? DistanceUnits.KILOMETERS : DistanceUnits.DEGREES;
    } else {
      this.distanceUnits = parseDistanceUnits(distanceUnitsStr);
      if (this.distanceUnits == null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Must specify distanceUnits as one of "+ DistanceUnits.getSupportedUnits() +
                " on field types with class "+getClass().getSimpleName());
    }

    final SupportedFormats fmts = ctx.getFormats();
    String format = args.remove(FORMAT);
    if (format == null) {
      format = "WKT";
    }
    shapeWriter = fmts.getWriter(format);
    shapeReader = fmts.getReader(format);
    if(shapeWriter==null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unknown Shape Format: "+ format);
    }
    if(shapeReader==null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unknown Shape Format: "+ format);
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
  public final Field createField(SchemaField field, Object val) {
    throw new IllegalStateException("instead call createFields() because isPolyField() is true");
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object val) {
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

    List<IndexableField> result = new ArrayList<>();
    if (field.indexed() || field.hasDocValues()) {
      T strategy = getStrategy(field.getName());
      result.addAll(Arrays.asList(strategy.createIndexableFields(shape)));
    }

    if (field.stored()) {
      result.add(new StoredField(field.getName(), getStoredValue(shape, shapeStr)));
    }

    return result;
  }

  /** Called by {@link #createFields(SchemaField, Object)} to get the stored value. */
  protected String getStoredValue(Shape shape, String shapeStr) {
    return (shapeStr == null) ? shapeToString(shape) : shapeStr;
  }

  /** Create a {@link Shape} from the input string */
  public Shape parseShape(String str) {
    str = str.trim();
    if (str.length() == 0)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "empty string shape");

    // If the first char is promising, try to parse with SpatialUtils.parsePoint
    char firstChar = str.charAt(0);
    if (firstChar == '+' || firstChar == '-' || (firstChar >= '0' && firstChar <= '9')) {
      try {
        return SpatialUtils.parsePoint(str, ctx);
      } catch (Exception e) {//ignore
      }
    }

    try {
      return shapeReader.read(str);
    } catch (Exception e) {
      String msg = "Unable to parse shape given formats" +
          " \"lat,lon\", \"x y\" or as " + shapeReader.getFormatName() + " because " + e;
      if (!msg.contains(str)) {
        msg += " input: " + str;
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg, e);
    }
  }

  /**
   * Returns a String version of a shape to be used for the stored value.
   *
   * The format can be selected using the initParam <code>format={WKT|GeoJSON}</code>
   */
  public String shapeToString(Shape shape) {
    return shapeWriter.toString(shape);
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
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
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
  protected Query getSpecializedExistenceQuery(QParser parser, SchemaField field) {
    PrefixQuery query = new PrefixQuery(new Term(field.getName(), ""));
    query.setRewriteMethod(field.getType().getRewriteMethod(parser, field));
    return query;
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
    //See SOLR-2883 needScore
    String scoreParam = (localParams == null ? null : localParams.get(SCORE_PARAM));

    //We get the valueSource for the score then the filter and combine them.
    DoubleValuesSource valueSource = getValueSourceFromSpatialArgs(parser, field, spatialArgs, scoreParam, strategy);
    if (valueSource == null) {
      return strategy.makeQuery(spatialArgs); //assumed constant scoring
    }

    FunctionScoreQuery functionQuery = new FunctionScoreQuery(new MatchAllDocsQuery(), valueSource);

    if (localParams != null && !localParams.getBool(FILTER_PARAM, true))
      return functionQuery;

    Query filterQuery = strategy.makeQuery(spatialArgs);
    return new BooleanQuery.Builder()
        .add(functionQuery, Occur.MUST)//matches everything and provides score
        .add(filterQuery, Occur.FILTER)//filters (score isn't used)
        .build();
  }

  @Override
  public double getSphereRadius() {
      return distanceUnits.getEarthRadius();
  }

  /** The set of values supported for the score local-param. Not null. */
  public Set<String> getSupportedScoreModes() {
    return supportedScoreModes;
  }

  protected DoubleValuesSource getValueSourceFromSpatialArgs(QParser parser, SchemaField field, SpatialArgs spatialArgs, String score, T strategy) {
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
      return fieldStrategyCache.get(fieldName, () -> newSpatialStrategy(fieldName));
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * @return The Spatial Context for this field type
   */
  public SpatialContext getSpatialContext() {
    return ctx;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
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


