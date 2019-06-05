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
package org.apache.solr.client.solrj.io.stream.eval;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.StatUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.NormalizeEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class NormalizeEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String,Object> values;

  public NormalizeEvaluatorTest() {
    super();
    factory = new StreamFactory().withFunctionName("norm", NormalizeEvaluator.class);
    values = new HashMap<String,Object>();
  }

  @Test
  public void test() throws IOException {
    
    int[] ints = new int[]{ 3, 5, 6, 7, 8 };
    long[] longs = new long[]{ 2L, 3L, 5L, 8L };
    double[] doubles = new double[]{ 3.4, 4.5, 6.7 };
    int[] singleInt = new int[]{ 6 };
    
    values.clear();
    values.put("ints", ints);
    values.put("longs", longs);
    values.put("doubles", doubles);
    values.put("singleInt", singleInt);
    Tuple tuple = new Tuple(values);

    assertSimilar(StatUtils.normalize(Arrays.stream(ints).mapToDouble(Double::valueOf).toArray()), factory.constructEvaluator("norm(ints)").evaluate(tuple));
    assertSimilar(StatUtils.normalize(Arrays.stream(longs).mapToDouble(Double::valueOf).toArray()), factory.constructEvaluator("norm(longs)").evaluate(tuple));
    assertSimilar(StatUtils.normalize(doubles), factory.constructEvaluator("norm(doubles)").evaluate(tuple));
    assertSimilar(StatUtils.normalize(Arrays.stream(singleInt).mapToDouble(Double::valueOf).toArray()), factory.constructEvaluator("norm(singleInt)").evaluate(tuple));
  }
  
  private void assertSimilar(double[] expected, Object actual){
    assertTrue(actual instanceof List<?>);
    assertEquals(0, ((List<?>)actual).stream().filter(item -> !(item instanceof Number)).count());
    
    double[] actualD = ((List<?>)actual).stream().mapToDouble(value -> ((Number)value).doubleValue()).toArray();
    Assert.assertTrue(Arrays.equals(expected, actualD));
  }

}
