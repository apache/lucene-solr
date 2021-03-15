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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.DoublePredicate;
import java.util.stream.Collectors;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionMatchQuery;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * syntax fq={!hash workers=11 worker=4 keys=field1,field2}
 */
public class HashQParserPlugin extends QParserPlugin {

  public static final String NAME = "hash";

  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new HashQParser(query, localParams, params, request);
  }

  private static class HashQParser extends QParser {

    public HashQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(query, localParams, params, request);
    }

    public Query parse() {
      int workers = localParams.getInt("workers", 0);
      if (workers < 2) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "workers needs to be more than 1");
      }
      int worker = localParams.getInt("worker", 0);
      String keyParam = params.get("partitionKeys");
      String[] keys = keyParam.replace(" ", "").split(",");
      Arrays.stream(keys).forEach(field -> req.getSchema().getField(field)); // validate all fields exist

      // TODO wish to provide matchCost on FunctionMatchQuery's TwoPhaseIterator -- LUCENE-9373
      return new FunctionMatchQuery(
          new HashCodeValuesSource(keys).toDoubleValuesSource(),
          new HashPartitionPredicate(workers, worker));
    }

  }

  /** A {@link LongValuesSource} that is a hash of some fields' values.  They are fetched via DocValues */
  private static class HashCodeValuesSource extends LongValuesSource {

    private final String[] fields;

    private HashCodeValuesSource(String[] fields) {
      this.fields = fields;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      // produce an array of the LongValues of the hash of each field
      final LongValues[] resultValues = new LongValues[fields.length];
      for (int i = 0; i < fields.length; i++) {
        final String field = fields[i];
        final NumericDocValues numericDocValues = ctx.reader().getNumericDocValues(field);
        if (numericDocValues != null) {
          // Numeric
          resultValues[i] = new LongValues() {
            // Even if not a Long field; could be int, double, float and this still works because DocValues numerics
            //  are based on a Long.
            final NumericDocValues values = numericDocValues;
            boolean atDoc = false;
            @Override
            public boolean advanceExact(int doc) throws IOException { atDoc = values.advanceExact(doc); return true; }
            @Override
            public long longValue() throws IOException { return atDoc ? Long.hashCode(values.longValue()) : 0; }
          };
          continue;
        }
        final SortedDocValues sortedDocValues = ctx.reader().getSortedDocValues(field);
        if (sortedDocValues != null) {
          // String
          resultValues[i] = new LongValues() {
            final SortedDocValues values = sortedDocValues;
            boolean atDoc = false;
            @Override
            public boolean advanceExact(int doc) throws IOException { atDoc = values.advanceExact(doc); return true; }
            @Override
            public long longValue() throws IOException {
              //TODO: maybe cache hashCode if same ord as prev doc to save lookupOrd?
              return atDoc ? values.lookupOrd(values.ordValue()).hashCode() : 0;
            }
          };
          continue;
        }
        // fail if some other DocValuesType is present
        final FieldInfo fieldInfo = ctx.reader().getFieldInfos().fieldInfo(field);
        if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't compute hash on field " + field);
        }
        // fall back on zero for empty case
        resultValues[i] = LongValuesSource.constant(0).getValues(ctx, scores);
      }

      // produce a hash LongValues of all those in this array
      if (resultValues.length == 1) {
        return resultValues[0];
      } else {
        // Combine
        return new LongValues() {
          private long result;
          @Override
          public boolean advanceExact(int doc) throws IOException {
            // compute the hash here.
            // algorithm borrowed from Arrays.hashCode(Object[]) but without needing to call hashCode redundantly
            result = 1;
            for (LongValues longValues : resultValues) {
              boolean present = longValues.advanceExact(doc);
              result = 31 * result + (present == false ? 0 : longValues.longValue());
            }
            return true; // we always have a hash value
          }
          @Override
          public long longValue() throws IOException { return result; }
        };
      }
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      HashCodeValuesSource that = (HashCodeValuesSource) o;
      return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fields);
    }

    @Override
    public String toString() {
      return Arrays.stream(fields).collect(Collectors.joining(",", "hash(", ")"));
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, fields);
    }
  }

  /**
   * Simple modulus check against the input to see if the result is a particular value
   * (standard hash partition approach).
   * Can't use a lambda because need equals/hashcode
   */
  private static class HashPartitionPredicate implements DoublePredicate {
    final int workers;
    final int worker;

    private HashPartitionPredicate(int workers, int worker) {
      this.workers = workers;
      this.worker = worker;
    }

    @Override
    public boolean test(double hashAsDouble) {
      return Math.abs((long)hashAsDouble) % workers == worker;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      HashPartitionPredicate that = (HashPartitionPredicate) o;
      return workers == that.workers &&
          worker == that.worker;
    }

    @Override
    public int hashCode() {
      return Objects.hash(workers, worker);
    }
  }

}
