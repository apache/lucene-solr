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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;

import org.apache.solr.common.MapWriter;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
class RangeVal implements MapWriter {
  final Number min, max, actual;

  RangeVal(Number min, Number max, Number actual) {
    this.min = min;
    this.max = max;
    this.actual = actual;
  }

  public boolean match(Number testVal) {
    if (testVal == null) return false;
    return Double.compare(testVal.doubleValue(), min.doubleValue()) >= 0 &&
        Double.compare(testVal.doubleValue(), max.doubleValue()) <= 0;
  }

  public Double realDelta(double v) {
    if (actual != null) return v - actual.doubleValue();
    else return delta(v);
  }

  public Double delta(double v) {
    if (v >= max.doubleValue()) return v - max.doubleValue();
    if (v <= min.doubleValue()) return v - min.doubleValue();
    return 0d;
  }

  @Override
  public String toString() {
    return jsonStr();
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("min", min)
        .put("max", max)
        .putIfNotNull("actual", actual);
  }
}
