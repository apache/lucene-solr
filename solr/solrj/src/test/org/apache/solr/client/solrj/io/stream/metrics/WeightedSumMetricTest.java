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

package org.apache.solr.client.solrj.io.stream.metrics;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.handler.SolrDefaultStreamFactory;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WeightedSumMetricTest {

  final long[] counts = new long[]{10, 20, 30, 40};
  final double[] avg = new double[]{2, 4, 6, 8};

  @Test
  public void testWsumPojo() throws Exception {
    WeightedSumMetric wsum = new WeightedSumMetric("avg", "count");
    assertEquals("wsum(avg, count, false)", wsum.getIdentifier());
    assertArrayEquals(new String[]{"avg", "count"}, wsum.getColumns());

    StreamFactory factory = new SolrDefaultStreamFactory();
    StreamExpressionParameter expr = wsum.toExpression(factory);
    assertTrue(expr instanceof StreamExpression);
    wsum = new WeightedSumMetric((StreamExpression) expr, factory);

    double expectedSum = 0d;
    for (int i = 0; i < counts.length; i++) {
      expectedSum += ((double) counts[i] / 100) * avg[i];
    }
    long expectedSumLong = Math.round(expectedSum);

    Number weightedSum = updateMetric(wsum);
    assertNotNull(weightedSum);
    assertTrue(weightedSum instanceof Double);
    assertTrue(weightedSum.doubleValue() == expectedSum);

    wsum = new WeightedSumMetric("avg", "count", true);
    assertEquals("wsum(avg, count, true)", wsum.getIdentifier());
    weightedSum = updateMetric(wsum);
    assertNotNull(weightedSum);
    assertTrue(weightedSum.longValue() == expectedSumLong);
  }

  private Number updateMetric(WeightedSumMetric wsum) {
    for (int i = 0; i < counts.length; i++) {
      Tuple t = new Tuple();
      t.put("avg", avg[i]);
      t.put("count", counts[i]);
      wsum.update(t);
    }
    return wsum.getValue();
  }
}
