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
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.collections.map.HashedMap;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.EqualsEvaluator;
import org.apache.solr.client.solrj.io.eval.RawValueEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

public class EqualsEvaluatorTest extends LuceneTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public EqualsEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("eq", EqualsEvaluator.class)
      .withFunctionName("val", RawValueEvaluator.class)
      ;
    values = new HashedMap();
  }
    
  @Test
  public void operationFieldName() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(sum(a),val(9))");    
    Object result;
    
    values.clear();
    values.put("sum(a)", 9);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);

  }
  
  @Test
  public void eqTwoIntegers() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
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
  }

  @Test
  public void eqTwoStrings() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
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
    Assert.assertEquals(false, false);
    
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
    Assert.assertEquals(false, result);
  }
  
  @Test
  public void eqTwoBooleans() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
    Object result;
    Boolean t = true;
    Boolean f = false;
    
    values.clear();
    values.put("a", true);
    values.put("b",true);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", false);
    values.put("b",false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", true);
    values.put("b", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, false);
    
    values.clear();
    values.put("a", false);
    values.put("b", true);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", t);
    values.put("b", f);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", t);
    values.put("b", t);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", f);
    values.put("b", f);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
  }

  @Test(expected = IOException.class)
  public void eqDifferentTypes1() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
    
    values.clear();
    values.put("a", true);
    values.put("b",1);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void eqDifferentTypes2() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
    
    values.clear();
    values.put("a", 1);
    values.put("b",false);
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test(expected = IOException.class)
  public void eqDifferentTypes3() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
    
    values.clear();
    values.put("a", "1");
    values.put("b",1);
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test(expected = IOException.class)
  public void eqDifferentTypes4() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eq(a,b)");
    
    values.clear();
    values.put("a", "true");
    values.put("b",true);
    evaluator.evaluate(new Tuple(values));
  }

}
