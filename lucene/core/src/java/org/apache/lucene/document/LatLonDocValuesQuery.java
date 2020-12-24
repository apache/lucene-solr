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

package org.apache.lucene.document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * Finds all previously indexed geo points that comply the given {@link ShapeField.QueryRelation}
 * with the specified array of {@link LatLonGeometry}.
 *
 * <p>The field must be indexed using {@link LatLonDocValuesField} added per document.
 */
class LatLonDocValuesQuery extends Query {

  private final String field;
  private final LatLonGeometry[] geometries;
  private final ShapeField.QueryRelation queryRelation;
  private final Component2D component2D;

  LatLonDocValuesQuery(
      String field, ShapeField.QueryRelation queryRelation, LatLonGeometry... geometries) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (queryRelation == null) {
      throw new IllegalArgumentException("queryRelation must not be null");
    }
    if (queryRelation == ShapeField.QueryRelation.WITHIN) {
      for (LatLonGeometry geometry : geometries) {
        if (geometry instanceof Line) {
          // TODO: line queries do not support within relations
          throw new IllegalArgumentException(
              "LatLonDocValuesPointQuery does not support "
                  + ShapeField.QueryRelation.WITHIN
                  + " queries with line geometries");
        }
      }
    }
    if (queryRelation == ShapeField.QueryRelation.CONTAINS) {
      for (LatLonGeometry geometry : geometries) {
        if ((geometry instanceof Point) == false) {
          throw new IllegalArgumentException(
              "LatLonDocValuesPointQuery does not support "
                  + ShapeField.QueryRelation.CONTAINS
                  + " queries with non-points geometries");
        }
      }
    }
    this.field = field;
    this.geometries = geometries;
    this.queryRelation = queryRelation;
    this.component2D = LatLonGeometry.create(geometries);
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (!this.field.equals(field)) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(queryRelation).append(':');
    sb.append("geometries(").append(Arrays.toString(geometries));
    return sb.append(")").toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    LatLonDocValuesQuery other = (LatLonDocValuesQuery) obj;
    return field.equals(other.field)
        && queryRelation == other.queryRelation
        && Arrays.equals(geometries, other.geometries);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + queryRelation.hashCode();
    h = 31 * h + Arrays.hashCode(geometries);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final GeoEncodingUtils.Component2DPredicate component2DPredicate =
        queryRelation == ShapeField.QueryRelation.CONTAINS
            ? null
            : GeoEncodingUtils.createComponentPredicate(component2D);
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final SortedNumericDocValues values = context.reader().getSortedNumericDocValues(field);
        if (values == null) {
          return null;
        }
        final TwoPhaseIterator iterator;
        switch (queryRelation) {
          case INTERSECTS:
            iterator = intersects(values, component2DPredicate);
            break;
          case WITHIN:
            iterator = within(values, component2DPredicate);
            break;
          case DISJOINT:
            iterator = disjoint(values, component2DPredicate);
            break;
          case CONTAINS:
            iterator = contains(values, geometries);
            break;
          default:
            throw new IllegalArgumentException(
                "Invalid query relationship:[" + queryRelation + "]");
        }
        return new ConstantScoreScorer(this, boost, scoreMode, iterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  private TwoPhaseIterator intersects(
      SortedNumericDocValues values, GeoEncodingUtils.Component2DPredicate component2DPredicate) {
    return new TwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final int lat = (int) (value >>> 32);
          final int lon = (int) (value & 0xFFFFFFFF);
          if (component2DPredicate.test(lat, lon)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public float matchCost() {
        return 1000f; // TODO: what should it be?
      }
    };
  }

  private TwoPhaseIterator within(
      SortedNumericDocValues values, GeoEncodingUtils.Component2DPredicate component2DPredicate) {
    return new TwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final int lat = (int) (value >>> 32);
          final int lon = (int) (value & 0xFFFFFFFF);
          if (component2DPredicate.test(lat, lon) == false) {
            return false;
          }
        }
        return true;
      }

      @Override
      public float matchCost() {
        return 1000f; // TODO: what should it be?
      }
    };
  }

  private TwoPhaseIterator disjoint(
      SortedNumericDocValues values, GeoEncodingUtils.Component2DPredicate component2DPredicate) {
    return new TwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final int lat = (int) (value >>> 32);
          final int lon = (int) (value & 0xFFFFFFFF);
          if (component2DPredicate.test(lat, lon)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public float matchCost() {
        return 1000f; // TODO: what should it be?
      }
    };
  }

  private TwoPhaseIterator contains(SortedNumericDocValues values, LatLonGeometry[] geometries) {
    final List<Component2D> component2Ds = new ArrayList<>(geometries.length);
    for (int i = 0; i < geometries.length; i++) {
      component2Ds.add(LatLonGeometry.create(geometries[i]));
    }
    return new TwoPhaseIterator(values) {
      @Override
      public boolean matches() throws IOException {
        Component2D.WithinRelation answer = Component2D.WithinRelation.DISJOINT;
        for (int i = 0, count = values.docValueCount(); i < count; ++i) {
          final long value = values.nextValue();
          final double lat = GeoEncodingUtils.decodeLatitude((int) (value >>> 32));
          final double lon = GeoEncodingUtils.decodeLongitude((int) (value & 0xFFFFFFFF));
          for (Component2D component2D : component2Ds) {
            Component2D.WithinRelation relation = component2D.withinPoint(lon, lat);
            if (relation == Component2D.WithinRelation.NOTWITHIN) {
              return false;
            } else if (relation != Component2D.WithinRelation.DISJOINT) {
              answer = relation;
            }
          }
        }
        return answer == Component2D.WithinRelation.CANDIDATE;
      }

      @Override
      public float matchCost() {
        return 1000f; // TODO: what should it be?
      }
    };
  }
}
