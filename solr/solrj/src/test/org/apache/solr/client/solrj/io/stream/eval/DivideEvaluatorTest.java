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
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.DivideEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class DivideEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public DivideEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("div", DivideEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void divTwoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.0/2, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.1/2, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.1/2.1, result);
  }

  @Test(expected = IOException.class)
  public void divOneField() throws Exception{
    factory.constructEvaluator("div(a)");
  }
  
  @Test(expected = IOException.class)
  public void divTwoFieldWithNulls() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    
    values.clear();
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test(expected = IOException.class)
  public void divTwoFieldsWithNullDenominator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    
    values.clear();
    values.put("a", 1);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void divTwoFieldsWithNullNumerator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    
    values.clear();
    values.put("b", 1);
    evaluator.evaluate(new Tuple(values));
  }


  @Test(expected = IOException.class)
  public void divTwoFieldsWithMissingDenominator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    
    values.clear();
    values.put("a", 1);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void divTwoFieldsWithMissingNumerator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    
    values.clear();
    values.put("b", 1);
    evaluator.evaluate(new Tuple(values));
  }

  
  @Test(expected = IOException.class)
  public void divManyFieldsWithValues() throws Exception{
    factory.constructEvaluator("div(a,b,c,d)");
  }
  
  @Test
  public void divManyFieldsWithSubdivs() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,div(b,c))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals((1.0 / (2.0 / 3)), result);
  }
  
  @Test(expected = IOException.class)
  public void divByZero() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    
    values.clear();
    values.put("a", 1);
    values.put("b", 0);
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test
  public void divZeroByValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("div(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 0);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(0D, result);
  }
}
