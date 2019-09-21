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
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

class DoubleRangeSlowRangeQuery extends BinaryRangeFieldRangeQuery {
  private final String field;
  private final double[] min;
  private final double[] max;

  DoubleRangeSlowRangeQuery(String field, double[] min, double[] max, RangeFieldQuery.QueryType queryType) {
    super(field, encodeRanges(min, max), DoubleRange.BYTES, min.length,
        queryType);
    this.field = field;
    this.min = min;
    this.max = max;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    DoubleRangeSlowRangeQuery that = (DoubleRangeSlowRangeQuery) obj;
    return Objects.equals(field, that.field)
        && Arrays.equals(min, that.min)
        && Arrays.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Arrays.hashCode(min);
    h = 31 * h + Arrays.hashCode(max);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b
        .append("[")
        .append(Arrays.toString(min))
        .append(" TO ")
        .append(Arrays.toString(max))
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return super.rewrite(reader);
  }

  private static byte[] encodeRanges(double[] min, double[] max) {
    byte[] result = new byte[2 * DoubleRange.BYTES * min.length];

    DoubleRange.verifyAndEncode(min, max, result);
    return result;
  }
}
