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
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.GreaterThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import org.junit.Assert;

public class GreaterThanEqualToEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public GreaterThanEqualToEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("gte", GreaterThanEqualToEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void gteTwoIntegers() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 1);
    values.put("b", 1.0);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 1.0);
    values.put("b", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", -1);
    values.put("b", -1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2.0);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", 1.0);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", 2);
    values.put("b", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 2);
    values.put("b", 1.0);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 2.0);
    values.put("b", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 3);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", -1);
    values.put("b", -2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 3);
    values.put("b", 2.0);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", 3.0);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
  }
  
  @Test
  public void gteTwoStrings() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");
    Object result;
    String foo = "foo";
    String bar = "bar";
    
    values.clear();
    values.put("a", "foo");
    values.put("b", "foo");
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", "foo");
    values.put("b", "bar");
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", "foo bar baz");
    values.put("b", "foo bar baz");
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", "foo bar baz");
    values.put("b", "foo bar jaz");
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", foo);
    values.put("b", foo);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", foo);
    values.put("b", bar);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
  }
  
  @Test(expected = IOException.class)
  public void gteTwoBooleans() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");

    values.clear();
    values.put("a", true);
    values.put("b",true);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void gteDifferentTypes1() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");
    
    values.clear();
    values.put("a", true);
    values.put("b",1);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void gteDifferentTypes2() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");
    
    values.clear();
    values.put("a", 1);
    values.put("b",false);
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test(expected = IOException.class)
  public void gteDifferentTypes3() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");
    
    values.clear();
    values.put("a", "1");
    values.put("b",1);
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test(expected = IOException.class)
  public void gteDifferentTypes4() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("gte(a,b)");
    
    values.clear();
    values.put("a", "true");
    values.put("b",true);
    evaluator.evaluate(new Tuple(values));
  }

}
