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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import  org.junit.Assert;

public class AddEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public AddEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("add", AddEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void addTwoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(3D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(3.1D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(3.2D, result);
  }

  @Test//(expected = NumberFormatException.class)
  public void addTwoFieldWithNulls() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(a,b)");
    Object result;
    
    values.clear();
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void addTwoFieldsWithNull() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", null);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", null);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("a", null);
    values.put("b", 1.1);    
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void addTwoFieldsWithMissingField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("a", 1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
    
    values.clear();
    values.put("b", 1.1);    
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test
  public void addManyFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(a,b,c,d)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(10D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(10.1D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(10.4D, result);
  }
  
  @Test
  public void addManyFieldsWithSubAdds() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(a,b,add(c,d))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(10D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(10.1D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(10.4D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.123456789123456);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(10.423456789123456, result);
    
    values.clear();
    values.put("a", 123456789123456789L);
    values.put("b", 123456789123456789L);
    values.put("c", 123456789123456789L);
    values.put("d", 123456789123456789L);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(4 * 123456789123456789D, result);
  }
  
  @Test
  public void addManyFieldsWithManySubAdds() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(add(a,b),add(c,d),add(c,a))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(14D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(14.2D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(14.6D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.123456789123456);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(14.623456789123456, result);
    
    values.clear();
    values.put("a", 123456789123456789L);
    values.put("b", 123456789123456789L);
    values.put("c", 123456789123456789L);
    values.put("d", 123456789123456789L);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(6 * 123456789123456789D, result);
    
    values.clear();
    values.put("a", 4.12345678);
    values.put("b", 4.12345678);
    values.put("c", 4.12345678);
    values.put("d", 4.12345678);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(6 * 4.12345678, result);
  }
  
  @Test
  public void addManyFieldsWithManySubAddsWithNegative() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(add(a,b),add(c,d),add(c,a))");
    Object result;
    
    values.clear();
    values.put("a", -1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(10D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    values.put("c", -3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(2.2D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", -3.1);
    values.put("d", 4.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(2.2D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    values.put("c", -3.1);
    values.put("d", 5.223456789123456);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(3.323456789123456, result);
    
    values.clear();
    values.put("a", 123456789123456789L);
    values.put("b", -123456789123456789L);
    values.put("c", 123456789123456789L);
    values.put("d", 123456789123456789L);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(4 * 123456789123456789D, result);
    
    values.clear();
    values.put("a", -4.12345678);
    values.put("b", -4.12345678);
    values.put("c", -4.12345678);
    values.put("d", -4.12345678);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(6 * -4.12345678, result);
  }

}
