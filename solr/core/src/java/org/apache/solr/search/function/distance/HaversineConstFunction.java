package org.apache.solr.search.function.distance;
/**
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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.ConstNumberSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.spatial.DistanceUtils;
import org.apache.lucene.spatial.tier.InvalidGeoException;
import org.apache.solr.common.params.SpatialParams;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.function.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Haversine function with one point constant
 */
public class HaversineConstFunction extends ValueSource {

  public static ValueSourceParser parser = new ValueSourceParser() {
    @Override
    public ValueSource parse(FunctionQParser fp) throws ParseException
    {
      // TODO: dispatch through SpatialQueriable in the future?
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
          throw new ParseException("geodist - invalid parameters:" + sources);
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
          mv2 = makeMV(sources.subList(1,3), sources);
        } else {                                   // (x,x,m)
          mv1 = makeMV(sources.subList(0,2), sources);
          vs1 = sources.get(2);
          if (!(vs1 instanceof MultiValueSource)) {
            throw new ParseException("geodist - invalid parameters:" + sources);
          }
          mv2 = (MultiValueSource)vs1;
        }
      } else if (sources.size()==4) {
        mv1 = makeMV(sources.subList(0,2), sources);
        mv2 = makeMV(sources.subList(2,4), sources);
      } else if (sources.size() > 4) {
        throw new ParseException("geodist - invalid parameters:" + sources);
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
        throw new ParseException("geodist - not enough parameters:" + sources);
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
  };

  /** make a MultiValueSource from two non MultiValueSources */
  private static VectorValueSource makeMV(List<ValueSource> sources, List<ValueSource> orig) throws ParseException {
    ValueSource vs1 = sources.get(0);
    ValueSource vs2 = sources.get(1);

    if (vs1 instanceof MultiValueSource || vs2 instanceof MultiValueSource) {
      throw new ParseException("geodist - invalid parameters:" + orig);
    }
    return  new VectorValueSource(sources);
  }

  private static MultiValueSource parsePoint(FunctionQParser fp) throws ParseException {
    String pt = fp.getParam(SpatialParams.POINT);
    if (pt == null) return null;
    double[] point = null;
    try {
      point = DistanceUtils.parseLatitudeLongitude(pt);
    } catch (InvalidGeoException e) {
      throw new ParseException("Bad spatial pt:" + pt);
    }
    return new VectorValueSource(Arrays.<ValueSource>asList(new DoubleConstValueSource(point[0]),new DoubleConstValueSource(point[1])));
  }

  private static double[] getConstants(MultiValueSource vs) {
    if (!(vs instanceof VectorValueSource)) return null;
    List<ValueSource> sources = ((VectorValueSource)vs).getSources();
    if (sources.get(0) instanceof ConstNumberSource && sources.get(1) instanceof ConstNumberSource) {
      return new double[] { ((ConstNumberSource) sources.get(0)).getDouble(), ((ConstNumberSource) sources.get(1)).getDouble()};
    }
    return null;
  }

  private static MultiValueSource parseSfield(FunctionQParser fp) throws ParseException {
    String sfield = fp.getParam(SpatialParams.FIELD);
    if (sfield == null) return null;
    SchemaField sf = fp.getReq().getSchema().getField(sfield);
    ValueSource vs = sf.getType().getValueSource(sf, fp);
    if (!(vs instanceof MultiValueSource)) {
      throw new ParseException("Spatial field must implement MultiValueSource:" + sf);
    }
    return (MultiValueSource)vs;
  }


  //////////////////////////////////////////////////////////////////////////////////////

  private final double latCenter;
  private final double lonCenter;
  private final VectorValueSource p2;  // lat+lon, just saved for display/debugging
  private final ValueSource latSource;
  private final ValueSource lonSource;

  private final double latCenterRad_cos; // cos(latCenter)
  private static final double EARTH_MEAN_DIAMETER = DistanceUtils.EARTH_MEAN_RADIUS_KM * 2;


  public HaversineConstFunction(double latCenter, double lonCenter, VectorValueSource vs) {
    this.latCenter = latCenter;
    this.lonCenter = lonCenter;
    this.p2 = vs;
    this.latSource = p2.getSources().get(0);
    this.lonSource = p2.getSources().get(1);
    this.latCenterRad_cos = Math.cos(latCenter * DistanceUtils.DEGREES_TO_RADIANS);
  }

  protected String name() {
    return "geodist";
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final FunctionValues latVals = latSource.getValues(context, readerContext);
    final FunctionValues lonVals = lonSource.getValues(context, readerContext);
    final double latCenterRad = this.latCenter * DistanceUtils.DEGREES_TO_RADIANS;
    final double lonCenterRad = this.lonCenter * DistanceUtils.DEGREES_TO_RADIANS;
    final double latCenterRad_cos = this.latCenterRad_cos;

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        double latRad = latVals.doubleVal(doc) * DistanceUtils.DEGREES_TO_RADIANS;
        double lonRad = lonVals.doubleVal(doc) * DistanceUtils.DEGREES_TO_RADIANS;
        double diffX = latCenterRad - latRad;
        double diffY = lonCenterRad - lonRad;
        double hsinX = Math.sin(diffX * 0.5);
        double hsinY = Math.sin(diffY * 0.5);
        double h = hsinX * hsinX +
                (latCenterRad_cos * Math.cos(latRad) * hsinY * hsinY);
        return (EARTH_MEAN_DIAMETER * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h)));
      }
      @Override
      public String toString(int doc) {
        return name() + '(' + latVals.toString(doc) + ',' + lonVals.toString(doc) + ',' + latCenter + ',' + lonCenter + ')';
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    latSource.createWeight(context, searcher);
    lonSource.createWeight(context, searcher);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HaversineConstFunction)) return false;
    HaversineConstFunction other = (HaversineConstFunction) o;
    return this.latCenter == other.latCenter
        && this.lonCenter == other.lonCenter
        && this.p2.equals(other.p2);

  }

  @Override
  public int hashCode() {
    int result = p2.hashCode();
    long temp;
    temp = Double.doubleToRawLongBits(latCenter);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToRawLongBits(lonCenter);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String description() {
    return name() + '(' + p2 + ',' + latCenter + ',' + lonCenter + ')';
  }
}
