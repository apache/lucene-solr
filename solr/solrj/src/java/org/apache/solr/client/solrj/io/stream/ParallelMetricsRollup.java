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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.io.stream.metrics.WeightedSumMetric;

/**
 * Indicates the underlying stream source supports parallelizing metrics computation across collections
 * using a rollup of metrics from each collection.
 */
public interface ParallelMetricsRollup {

  TupleStream[] parallelize(List<String> partitions) throws IOException;

  StreamComparator getParallelListSortOrder() throws IOException;

  RollupStream getRollupStream(SortStream sortStream, Metric[] rollupMetrics) throws IOException;

  Map<String, String> getRollupSelectFields(Metric[] rollupMetrics);

  default Optional<TupleStream> openParallelStream(StreamContext context, List<String> partitions, Metric[] metrics) throws IOException {
    Optional<Metric[]> maybeRollupMetrics = getRollupMetrics(metrics);
    if (maybeRollupMetrics.isEmpty())
      return Optional.empty(); // some metric is incompatible with doing a rollup over the plist results

    TupleStream[] parallelStreams = parallelize(partitions);
    // the tuples from each plist need to be sorted using the same order to do a rollup
    Metric[] rollupMetrics = maybeRollupMetrics.get();
    StreamComparator comparator = getParallelListSortOrder();
    SortStream sortStream = new SortStream(new ParallelListStream(parallelStreams), comparator);
    RollupStream rollup = getRollupStream(sortStream, rollupMetrics);
    SelectStream select = new SelectStream(rollup, getRollupSelectFields(rollupMetrics));
    select.setStreamContext(context);
    select.open();

    return Optional.of(select);
  }

  default Optional<Metric[]> getRollupMetrics(Metric[] metrics) {
    Metric[] rollup = new Metric[metrics.length];
    CountMetric count = null;
    for (int m = 0; m < rollup.length; m++) {
      Metric nextRollup;
      Metric next = metrics[m];
      if (next instanceof SumMetric) {
        // sum of sums
        nextRollup = new SumMetric(next.getIdentifier());
      } else if (next instanceof MinMetric) {
        // min of mins
        nextRollup = new MinMetric(next.getIdentifier());
      } else if (next instanceof MaxMetric) {
        // max of max
        nextRollup = new MaxMetric(next.getIdentifier());
      } else if (next instanceof CountMetric) {
        // sum of counts
        nextRollup = new SumMetric(next.getIdentifier());
        count = (CountMetric) next;
      } else if (next instanceof MeanMetric) {
        // WeightedSumMetric must have a count to compute the weighted avg. rollup from ...
        // if the user is not requesting count, then we can't parallelize
        if (count == null) {
          // just look past the current position
          for (int n = m + 1; n < metrics.length; n++) {
            if (metrics[n] instanceof CountMetric) {
              count = (CountMetric) metrics[n];
              break;
            }
          }
        }
        if (count != null) {
          nextRollup = new WeightedSumMetric(next.getIdentifier(), count.getIdentifier());
        } else {
          return Optional.empty(); // can't properly rollup mean metrics w/o a count (reqd by WeightedSumMetric)
        }
      } else {
        return Optional.empty(); // can't parallelize this expr!
      }

      rollup[m] = nextRollup;
    }

    return Optional.of(rollup);
  }
}
