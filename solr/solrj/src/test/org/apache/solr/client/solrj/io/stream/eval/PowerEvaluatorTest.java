/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for multitional information regarding copyright ownership.
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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.PowerEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class PowerEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public PowerEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("pow", PowerEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void powTwoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("pow(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 2);
    values.put("b", 5);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Number);
    Assert.assertEquals(BigDecimal.valueOf(Math.pow(2, 5)), BigDecimal.valueOf(result instanceof Long ? (long)result : (double)result));
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Number);
    Assert.assertEquals(Math.pow(1.1, 2), result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Number);
    Assert.assertEquals(Math.pow(1.1, 2.1), result);
    
    values.clear();
    values.put("a", -1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(Double.isNaN((double)result));
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", -2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Number);
    Assert.assertEquals(Math.pow(1.1, -2.1), result);
    
    values.clear();
    values.put("a", -1.1);
    values.put("b", -2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(Double.isNaN((double)result));
  }

  @Test(expected = IOException.class)
  public void powOneField() throws Exception{
    factory.constructEvaluator("pow(a)");
  }

  @Test//(expected = NumberFormatException.class)
  public void powTwoFieldWithNulls() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("pow(a,b)");
    
    values.clear();
    Assert.assertNull(evaluator.evaluate(new Tuple(values)));
  }

  @Test
  public void powManyFieldsWithSubpows() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("pow(a,pow(b,c))");
    Object result;
    
    values.clear();
    values.put("a", 8);
    values.put("b", 2);
    values.put("c", 3);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Number);
    Assert.assertEquals(BigDecimal.valueOf(Math.pow(8, Math.pow(2, 3))), BigDecimal.valueOf(result instanceof Long ? (long)result : (double)result));
  }
  
}
