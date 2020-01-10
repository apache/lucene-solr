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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.MultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class MultiplyEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public MultiplyEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("mult", MultiplyEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void multTwoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(2D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(2.2D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(2.31D, result);
  }

  @Test
  public void multOneField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a)");
    Object result;
    
    values.clear();
    values.put("a", 6);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(6D, result);
    
    values.clear();
    values.put("a", 6.5);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(6.5D, result);
  }

  @Test//(expected = NumberFormatException.class)
  public void multTwoFieldWithNulls() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a,b)");
    Object result;
    
    values.clear();
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void multTwoFieldsWithNull() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", null);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);

    values.clear();
    values.put("a", null);
    values.put("b", 1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", null);    
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void multTwoFieldsWithMissingField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("b", 1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("a", 1.1);    
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test
  public void multManyFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a,b,c,d)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(24D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(26.4D, result);
    
    values.clear();
    values.put("a", 10.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(269.5791D, result);
  }
  
  @Test
  public void multManyFieldsWithSubmults() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mult(a,b,mult(c,d))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(24D, result);
  }
}
