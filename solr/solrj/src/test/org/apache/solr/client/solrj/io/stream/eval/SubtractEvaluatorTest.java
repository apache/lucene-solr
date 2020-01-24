/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for subitional information regarding copyright ownership.
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
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.eval.SubtractEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class SubtractEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public SubtractEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("sub", SubtractEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void subTwoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("sub(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(-1D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(-.9D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(-1D, result);
  }

  @Test(expected = IOException.class)
  public void subOneField() throws Exception{
    factory.constructEvaluator("sub(a)");
  }

  @Test//(expected = NumberFormatException.class)
  public void subTwoFieldWithNulls() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("sub(a,b)");
    Object result;
    
    values.clear();
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void subTwoFieldsWithNull() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("sub(a,b)");
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
    values.put("a", 1.1);
    values.put("b", null);    
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void subTwoFieldsWithMissingField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("sub(a,b)");
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
    values.put("a", 1.1);    
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }

  @Test
  public void subManyFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("sub(a,b,c,d)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(-8D, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(-7.9D, result);
    
    values.clear();
    values.put("a", 10.1);
    values.put("b", 2.1);
    values.put("c", 3.1);
    values.put("d", 4.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(.8D, result);
  }
  
  @Test
  public void subManyFieldsWithSubsubs() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("sub(a,b,sub(c,d))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 3);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(0D, result);
        
    values.clear();
    values.put("a", 123456789123456789L);
    values.put("b", 123456789123456789L);
    values.put("c", 123456789123456789L);
    values.put("d", 123456789123456789L);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(0D, result);
  }
}
