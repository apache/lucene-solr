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

  /**
   * Given a list of collections, return an array of TupleStream for each partition.
   * @param partitions A list of collections to parallelize metrics computation across.
   * @return An array of TupleStream for each partition requested.
   * @throws IOException if an error occurs while constructing the underlying TupleStream for a partition.
   */
  TupleStream[] parallelize(List<String> partitions) throws IOException;

  /**
   * Get a Tuple sorter needed for rolling up over the parallelized streams.
   * @return A comparator for sorting the parallel streams needed for doing a rollup.
   * @throws IOException if a parsing error occurs
   */
  StreamComparator getRollupSorter() throws IOException;

  /**
   * Get the rollup for the parallelized streams.
   * @param sortStream A sorted stream to perform the rollup on
   * @param rollupMetrics An array of metrics to rollup
   * @return A rollup over parallelized streams that provide metrics
   * @throws IOException if an error occurs while reading from the sorted stream
   */
  RollupStream getRollupStream(SortStream sortStream, Metric[] rollupMetrics) throws IOException;

  /**
   * The projection of dimensions and metrics from the rollup stream.
   * @param rollupMetrics The metrics being rolled up.
   * @return A mapping of fields produced by the rollup stream to their output name.
   */
  Map<String, String> getRollupSelectFields(Metric[] rollupMetrics);

  /**
   * Given a list of partitions (collections), open a select stream that projects the dimensions and
   * metrics produced by rolling up over a parallelized group of streams. If it's not possible to rollup
   * the metrics produced by the underlying metrics stream, this method returns Optional.empty.
   * @param context The current streaming expression context
   * @param partitions A list of collections to parallelize metrics computation across.
   * @param metrics A list of metrics to rollup.
   * @return Either a TupleStream that performs a rollup over parallelized streams or empty.
   * @throws IOException if an error occurs reading tuples from the parallelized streams
   */
  default Optional<TupleStream> openParallelStream(StreamContext context, List<String> partitions, Metric[] metrics) throws IOException {
    Optional<Metric[]> maybeRollupMetrics = getRollupMetrics(metrics);
    if (maybeRollupMetrics.isEmpty())
      return Optional.empty(); // some metric is incompatible with doing a rollup over the plist results

    TupleStream[] parallelStreams = parallelize(partitions);
    // the tuples from each plist need to be sorted using the same order to do a rollup
    Metric[] rollupMetrics = maybeRollupMetrics.get();
    StreamComparator comparator = getRollupSorter();
    SortStream sortStream = new SortStream(new ParallelListStream(parallelStreams), comparator);
    RollupStream rollup = getRollupStream(sortStream, rollupMetrics);
    SelectStream select = new SelectStream(rollup, getRollupSelectFields(rollupMetrics));
    select.setStreamContext(context);
    select.open();

    return Optional.of(select);
  }

  /**
   * Either an array of metrics that can be parallelized and rolled up or empty.
   * @param metrics The list of metrics that we want to parallelize.
   * @return Either an array of metrics that can be parallelized and rolled up or empty.
   */
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
