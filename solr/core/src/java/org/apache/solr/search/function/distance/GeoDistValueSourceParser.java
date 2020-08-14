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
package org.apache.solr.search.function.distance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstNumberSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SpatialParams;
import org.apache.solr.schema.AbstractSpatialFieldType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.util.DistanceUnits;
import org.apache.solr.util.SpatialUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

/**
 * Parses "geodist" creating {@link HaversineConstFunction} or {@link HaversineFunction}
 * or calling {@link SpatialStrategy#makeDistanceValueSource(org.locationtech.spatial4j.shape.Point,double)}.
 */
public class GeoDistValueSourceParser extends ValueSourceParser {

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    // TODO: dispatch through SpatialQueryable in the future?

    //note: parseValueSourceList can't handle a field reference to an AbstractSpatialFieldType,
    // so those fields are expressly handled via sfield=
    List<ValueSource> sources;
    try {
      sources = fp.parseValueSourceList();
    } catch (SolrException e) {
      if (e.getMessage().equals("A ValueSource isn't directly available from this field. " +
          "Instead try a query using the distance as the score.")) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "geodist() does not support field names in its arguments " +
            "when stated fields are solr.LatLonPointSpatialField spatial type, requires sfield param instead");
      }
      else {
        throw e;
      }
    }

    // "m" is a multi-value source, "x" is a single-value source
    // allow (m,m) (m,x,x) (x,x,m) (x,x,x,x)
    // if not enough points are present, "pt" will be checked first, followed by "sfield".

    MultiValueSource mv1 = null;
    MultiValueSource mv2 = null;

    if (sources.size() == 0) {
      // nothing to do now
    } else if (sources.size() == 1) {
      ValueSource vs = sources.get(0);
      if (!(vs instanceof MultiValueSource)) {
        throw new SyntaxError("geodist - invalid parameters:" + sources);
      }
      mv1 = (MultiValueSource)vs;
    } else if (sources.size() == 2) {
      ValueSource vs1 = sources.get(0);
      ValueSource vs2 = sources.get(1);

      if (vs1 instanceof MultiValueSource && vs2 instanceof MultiValueSource) {
        mv1 = (MultiValueSource)vs1;
        mv2 = (MultiValueSource)vs2;
      } else {
        mv1 = makeMV(sources, sources);
      }
    } else if (sources.size()==3) {
      ValueSource vs1 = sources.get(0);
      ValueSource vs2 = sources.get(1);
      if (vs1 instanceof MultiValueSource) {     // (m,x,x)
        mv1 = (MultiValueSource)vs1;
        mv2 = makeMV(sources.subList(1, 3), sources);
      } else {                                   // (x,x,m)
        mv1 = makeMV(sources.subList(0, 2), sources);
        vs1 = sources.get(2);
        if (!(vs1 instanceof MultiValueSource)) {
          throw new SyntaxError("geodist - invalid parameters:" + sources);
        }
        mv2 = (MultiValueSource)vs1;
      }
    } else if (sources.size()==4) {
      mv1 = makeMV(sources.subList(0, 2), sources);
      mv2 = makeMV(sources.subList(2, 4), sources);
    } else if (sources.size() > 4) {
      throw new SyntaxError("geodist - invalid parameters:" + sources);
    }

    if (mv1 == null) {
      mv1 = parsePoint(fp);
      mv2 = parseSfield(fp);
    } else if (mv2 == null) {
      mv2 = parsePoint(fp);
      if (mv2 == null)
        mv2 = parseSfield(fp);
    }

    if (mv1 == null || mv2 == null) {
      throw new SyntaxError("geodist - not enough parameters:" + sources);
    }

    // We have all the parameters at this point, now check if one of the points is constant
    double[] constants;//latLon
    constants = getConstants(mv1);
    MultiValueSource other = mv2;
    if (constants == null) {
      constants = getConstants(mv2);
      other = mv1;
    }

    // At this point we dispatch to one of:
    // * SpatialStrategy.makeDistanceValueSource
    // * HaversineConstFunction
    // * HaversineFunction

    // sfield can only be in mv2, according to the logic above
    if (mv2 instanceof SpatialStrategyMultiValueSource) {
      if (constants == null)
        throw new SyntaxError("When using AbstractSpatialFieldType (e.g. RPT not LatLonType)," +
            " the point must be supplied as constants");
      // note: uses Haversine by default but can be changed via distCalc=...
      SpatialStrategy strategy = ((SpatialStrategyMultiValueSource) mv2).strategy;
      DistanceUnits distanceUnits = ((SpatialStrategyMultiValueSource) mv2).distanceUnits;
      Point queryPoint = strategy.getSpatialContext().makePoint(constants[1], constants[0]);
      return ValueSource.fromDoubleValuesSource(strategy.makeDistanceValueSource(queryPoint, distanceUnits.multiplierFromDegreesToThisUnit()));
    }

    if (constants != null && other instanceof VectorValueSource) {
      return new HaversineConstFunction(constants[0], constants[1], (VectorValueSource)other);
    }

    return new HaversineFunction(mv1, mv2, DistanceUtils.EARTH_MEAN_RADIUS_KM, true);
  }

  /** make a MultiValueSource from two non MultiValueSources */
  private VectorValueSource makeMV(List<ValueSource> sources, List<ValueSource> orig) throws SyntaxError {
    ValueSource vs1 = sources.get(0);
    ValueSource vs2 = sources.get(1);

    if (vs1 instanceof MultiValueSource || vs2 instanceof MultiValueSource) {
      throw new SyntaxError("geodist - invalid parameters:" + orig);
    }
    return  new VectorValueSource(sources);
  }

  private MultiValueSource parsePoint(FunctionQParser fp) throws SyntaxError {
    String ptStr = fp.getParam(SpatialParams.POINT);
    if (ptStr == null) return null;
    Point point = SpatialUtils.parsePointSolrException(ptStr, SpatialContext.GEO);
    //assume Lat Lon order
    return new VectorValueSource(
        Arrays.<ValueSource>asList(new DoubleConstValueSource(point.getY()), new DoubleConstValueSource(point.getX())));
  }

  private double[] getConstants(MultiValueSource vs) {
    if (!(vs instanceof VectorValueSource)) return null;
    List<ValueSource> sources = ((VectorValueSource)vs).getSources();
    if (sources.get(0) instanceof ConstNumberSource && sources.get(1) instanceof ConstNumberSource) {
      return new double[] { ((ConstNumberSource) sources.get(0)).getDouble(), ((ConstNumberSource) sources.get(1)).getDouble()};
    }
    return null;
  }

  private MultiValueSource parseSfield(FunctionQParser fp) throws SyntaxError {
    String sfield = fp.getParam(SpatialParams.FIELD);
    if (sfield == null) return null;
    SchemaField sf = fp.getReq().getSchema().getField(sfield);
    FieldType type = sf.getType();
    if (type instanceof AbstractSpatialFieldType) {
      AbstractSpatialFieldType asft = (AbstractSpatialFieldType) type;
      return new SpatialStrategyMultiValueSource(asft.getStrategy(sfield), asft.getDistanceUnits());
    }
    ValueSource vs = type.getValueSource(sf, fp);
    if (vs instanceof MultiValueSource) {
      return (MultiValueSource)vs;
    }
    throw new SyntaxError("Spatial field must implement MultiValueSource or extend AbstractSpatialFieldType:" + sf);
  }

  /** An unfortunate hack to use a {@link SpatialStrategy} instead of
   * a ValueSource. */
  private static class SpatialStrategyMultiValueSource extends VectorValueSource {

    final SpatialStrategy strategy;
    final DistanceUnits distanceUnits;

    public SpatialStrategyMultiValueSource(SpatialStrategy strategy, DistanceUnits distanceUnits) {
      super(Collections.EMPTY_LIST);
      this.strategy = strategy;
      this.distanceUnits = distanceUnits;
    }

    @Override
    public List<ValueSource> getSources() {
      throw new IllegalStateException();
    }
  }

}
