package org.apache.solr.search.function.distance;

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

import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.io.ParseUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstNumberSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.solr.common.params.SpatialParams;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

import java.util.Arrays;
import java.util.List;

/**
 * Parses "geodist" creating {@link HaversineConstFunction} or {@link HaversineFunction}.
 */
public class GeoDistValueSourceParser extends ValueSourceParser {

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    // TODO: dispatch through SpatialQueryable in the future?
    List<ValueSource> sources = fp.parseValueSourceList();

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
    double[] constants;
    constants = getConstants(mv1);
    MultiValueSource other = mv2;
    if (constants == null) {
      constants = getConstants(mv2);
      other = mv1;
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
    String pt = fp.getParam(SpatialParams.POINT);
    if (pt == null) return null;
    double[] point = null;
    try {
      point = ParseUtils.parseLatitudeLongitude(pt);
    } catch (InvalidShapeException e) {
      throw new SyntaxError("Bad spatial pt:" + pt);
    }
    return new VectorValueSource(Arrays.<ValueSource>asList(new DoubleConstValueSource(point[0]), new DoubleConstValueSource(point[1])));
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
    ValueSource vs = sf.getType().getValueSource(sf, fp);
    if (!(vs instanceof MultiValueSource)) {
      throw new SyntaxError("Spatial field must implement MultiValueSource:" + sf);
    }
    return (MultiValueSource)vs;
  }

}
