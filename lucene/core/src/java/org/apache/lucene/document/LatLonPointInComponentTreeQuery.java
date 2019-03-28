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
import java.util.Objects;

import org.apache.lucene.geo.ComponentTree;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;

/** Finds all previously indexed points that fall within the specified polygons.
 *
 *  <p>The field must be indexed with using {@link LatLonPoint} added per document.
 *
 *  @lucene.experimental */

final class LatLonPointInComponentTreeQuery extends Query {
  final String field;
  final ComponentTree componentTree;

  LatLonPointInComponentTreeQuery(String field, ComponentTree componentTree) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.field = field;
    this.componentTree = componentTree;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        LatLonPoint.checkCompatible(fieldInfo);

        // matching docids
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);

        values.intersect( 
                         new IntersectVisitor() {

                           DocIdSetBuilder.BulkAdder adder;

                           @Override
                           public void grow(int count) {
                             adder = result.grow(count);
                           }

                           @Override
                           public void visit(int docID) {
                             adder.add(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             double lat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(packedValue, 0));
                             double lon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES));
                             if (componentTree.contains(lat, lon)) {
                               adder.add(docID);
                             }
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

                             double cellMinLat = decodeLatitude(minPackedValue, 0);
                             double cellMinLon = decodeLongitude(minPackedValue, Integer.BYTES);
                             double cellMaxLat = decodeLatitude(maxPackedValue, 0);
                             double cellMaxLon = decodeLongitude(maxPackedValue, Integer.BYTES);
                             return componentTree.relate(cellMinLat, cellMaxLat, cellMinLon, cellMaxLon);
                           }
                         });

        return new ConstantScoreScorer(this, score(), scoreMode, result.build().iterator());
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /** Returns the query field */
  public String getField() {
    return field;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + field.hashCode();
    result = prime * result + Objects.hashCode(componentTree);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LatLonPointInComponentTreeQuery other) {
    return field.equals(other.field) &&
           Objects.equals(componentTree, other.componentTree);
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(Objects.toString(componentTree));
    return sb.toString();
  }
}
