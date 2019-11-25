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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.AscEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import org.junit.Assert;

public class AscEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public AscEvaluatorTest() {
    super();

    factory = new StreamFactory()
        .withFunctionName("asc", AscEvaluator.class);
      values = new HashMap<String,Object>();
    }
      
    @Test
    public void integerSortTest() throws Exception{
      StreamEvaluator evaluator = factory.constructEvaluator("asc(a)");
      Object result;
      
      values.clear();
      values.put("a", Arrays.asList(2,4,1,3,5,8,7));
      result = evaluator.evaluate(new Tuple(values));
      Assert.assertTrue(result instanceof List<?>);
      Assert.assertEquals(7, ((List<?>)result).size());
      checkOrder(Arrays.asList(1D,2D,3D,4D,5D,7D,8D), (List<Object>)result);
    }

    @Test
    public void doubleSortTest() throws Exception{
      StreamEvaluator evaluator = factory.constructEvaluator("asc(a)");
      Object result;
      
      values.clear();
      values.put("a", Arrays.asList(2.3, 2.1, 2.7, 2.6, 2.5));
      result = evaluator.evaluate(new Tuple(values));
      Assert.assertTrue(result instanceof List<?>);
      Assert.assertEquals(5, ((List<?>)result).size());
      checkOrder(Arrays.asList(2.1, 2.3, 2.5, 2.6, 2.7), (List<Object>)result);
    }

    @Test
    public void doubleWithIntegersSortTest() throws Exception{
      StreamEvaluator evaluator = factory.constructEvaluator("asc(a)");
      Object result;
      
      values.clear();
      values.put("a", Arrays.asList(2.3, 2.1, 2.0, 2.7, 2.6, 2.5, 3));
      result = evaluator.evaluate(new Tuple(values));
      Assert.assertTrue(result instanceof List<?>);
      Assert.assertEquals(7, ((List<?>)result).size());
      checkOrder(Arrays.asList(2D, 2.1, 2.3, 2.5, 2.6, 2.7, 3D), (List<Object>)result);
    }

    @Test
    public void stringSortTest() throws Exception{
      StreamEvaluator evaluator = factory.constructEvaluator("asc(a)");
      Object result;
      
      values.clear();
      values.put("a", Arrays.asList("a","c","b","e","d"));
      result = evaluator.evaluate(new Tuple(values));
      Assert.assertTrue(result instanceof List<?>);
      Assert.assertEquals(5, ((List<?>)result).size());
      checkOrder(Arrays.asList("a","b","c","d","e"), (List<Object>)result);
    }

    private <T> void checkOrder(List<?> expected, List<?> actual){
      Assert.assertEquals(expected.size(), actual.size());
      for(int idx = 0; idx < expected.size(); ++idx){
        Comparable<Object> expectedValue = (Comparable<Object>)expected.get(idx);
        Comparable<Object> actualValue = (Comparable<Object>)actual.get(idx);
        
        Assert.assertEquals(0, expectedValue.compareTo(actualValue));
      }
    }
}
