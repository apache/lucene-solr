package org.apache.lucene.facet;

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

import java.util.Arrays;
import java.util.List;

public final class FacetResult {

  /** Total value for this path (sum of all child counts, or
   *  sum of all child values), even those not included in
   *  the topN. */
  public final Number value;

  /** How many labels were populated under the requested
   *  path. */
  public final int childCount;

  /** Child counts. */
  public final LabelAndValue[] labelValues;

  public FacetResult(Number value, LabelAndValue[] labelValues, int childCount) {
    this.value = value;
    this.labelValues = labelValues;
    this.childCount = childCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("value=");
    sb.append(value);
    sb.append(" childCount=");
    sb.append(childCount);
    sb.append('\n');
    for(LabelAndValue labelValue : labelValues) {
      sb.append("  " + labelValue + "\n");
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

  // nocommit hashCode
}
