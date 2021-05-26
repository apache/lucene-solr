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
package org.apache.lucene.facet;

import java.util.Arrays;

/** Counts or aggregates for a single dimension. */
public final class FacetResult {

  /** Dimension that was requested. */
  public final String dim;

  /** Path whose children were requested. */
  public final String[] path;

  /**
   * Total number of documents containing a value for this path, even those not included in the
   * topN. If a document contains multiple values for the same path, it will only be counted once in
   * this value.
   */
  // TODO: This may not hold true for SSDV faceting, where docs can be counted more than
  // once. We should fix this. See LUCENE-9952
  public final Number value;

  /** How many child labels were encountered. */
  public final int childCount;

  /** Child counts. */
  public final LabelAndValue[] labelValues;

  /** Sole constructor. */
  public FacetResult(String dim, String[] path, Number value, LabelAndValue[] labelValues, int childCount) {
    this.dim = dim;
    this.path = path;
    this.value = value;
    this.labelValues = labelValues;
    this.childCount = childCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("dim=");
    sb.append(dim);
    sb.append(" path=");
    sb.append(Arrays.toString(path));
    sb.append(" value=");
    sb.append(value);
    sb.append(" childCount=");
    sb.append(childCount);
    sb.append('\n');
    for(LabelAndValue labelValue : labelValues) {
      sb.append("  ").append(labelValue).append("\n");
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object _other) {
    if ((_other instanceof FacetResult) == false) {
      return false;
    }
    FacetResult other = (FacetResult) _other;
    return value.equals(other.value) &&
      childCount == other.childCount &&
      Arrays.equals(labelValues, other.labelValues);
  }

  @Override
  public int hashCode() {
    int hashCode = value.hashCode() + 31 * childCount;
    for(LabelAndValue labelValue : labelValues) {
      hashCode = labelValue.hashCode() + 31 * hashCode;
    }
    return hashCode;
  }
}
