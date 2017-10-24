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

package org.apache.lucene.util;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public interface FieldQueryBuilder {

  Query newTermQuery(Term term);

  Query newRangeQuery(String field, String lower, String upper, boolean includeLower, boolean includeUpper);

  FieldQueryBuilder LONG_POINT_BUILDER = new FieldQueryBuilder() {

    private long[] parseLongArray(String inputs, long limit) {
      String[] parts = inputs.split(",");
      long[] outputs = new long[parts.length];
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].equals("*"))
          outputs[i] = limit;
        else
          outputs[i] = Long.parseLong(parts[i]);
      }
      return outputs;
    }

    @Override
    public Query newTermQuery(Term term) {
      if (term.text().equals("*"))
        return new TermQuery(term);
      long[] points = parseLongArray(term.text(), 0);
      return LongPoint.newRangeQuery(term.field(), points, points);
    }

    @Override
    public Query newRangeQuery(String field, String lower, String upper, boolean includeLower, boolean includeUpper) {
      long[] lowerVals = parseLongArray(lower, Long.MIN_VALUE);
      if (includeLower == false) {
        for (int i = 0; i < lowerVals.length; i++) {
          lowerVals[i] = lowerVals[i] + 1;
        }
      }
      long upperVals[] = parseLongArray(upper, Long.MAX_VALUE);
      if (includeUpper == false) {
        for (int i = 0; i < upperVals.length; i++) {
          upperVals[i] = upperVals[i] - 1;
        }
      }
      return LongPoint.newRangeQuery(field, lowerVals, upperVals);
    }
  };

  FieldQueryBuilder DOUBLE_POINT_BUILDER = new FieldQueryBuilder() {

    private double[] parseDoubleArray(String inputs, double limit) {
      String[] parts = inputs.split(",");
      double[] outputs = new double[parts.length];
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].equals("*"))
          outputs[i] = limit;
        else
          outputs[i] = Double.parseDouble(parts[i]);
      }
      return outputs;
    }

    @Override
    public Query newTermQuery(Term term) {
      if (term.text().equals("*"))
        return new TermQuery(term);
      double[] points = parseDoubleArray(term.text(), 0);
      return DoublePoint.newRangeQuery(term.field(), points, points);
    }

    @Override
    public Query newRangeQuery(String field, String lower, String upper, boolean includeLower, boolean includeUpper) {
      double[] lowerVals = parseDoubleArray(lower, Double.MIN_VALUE);
      if (includeLower == false) {
        for (int i = 0; i < lowerVals.length; i++) {
          lowerVals[i] = Math.nextUp(lowerVals[i]);
        }
      }
      double upperVals[] = parseDoubleArray(upper, Double.MAX_VALUE);
      if (includeUpper == false) {
        for (int i = 0; i < upperVals.length; i++) {
          upperVals[i] = Math.nextDown(lowerVals[i]);
        }
      }
      return DoublePoint.newRangeQuery(field, lowerVals, upperVals);
    }
  };

}
