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
import org.apache.solr.client.solrj.io.eval.ModuloEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class ModuloEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public ModuloEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("mod", ModuloEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void modTwoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1 % 2, ((Number)result).doubleValue(), 0.0);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1.1 % 2, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1.1 % 2.1, result);
  }

  @Test(expected = IOException.class)
  public void modOneField() throws Exception{
    factory.constructEvaluator("mod(a)");
  }
  
  @Test(expected = IOException.class)
  public void modTwoFieldWithNulls() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    
    values.clear();
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test(expected = IOException.class)
  public void modTwoFieldsWithNullDenominator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    
    values.clear();
    values.put("a", 1);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void modTwoFieldsWithNullNumerator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    
    values.clear();
    values.put("b", 1);
    evaluator.evaluate(new Tuple(values));
  }


  @Test(expected = IOException.class)
  public void modTwoFieldsWithMissingDenominator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    
    values.clear();
    values.put("a", 1);
    evaluator.evaluate(new Tuple(values));
  }

  @Test(expected = IOException.class)
  public void modTwoFieldsWithMissingNumerator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    
    values.clear();
    values.put("b", 1);
    evaluator.evaluate(new Tuple(values));
  }

  
  @Test(expected = IOException.class)
  public void modManyFieldsWithValues() throws Exception{
    factory.constructEvaluator("mod(a,b,c,d)");
  }
  
  @Test
  public void modManyFieldsWithSubmods() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,mod(b,c))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", 2);
    values.put("c", 9);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1 % (2 % 9), ((Number)result).doubleValue(), 0.0);
  }
  
  @Test(expected = IOException.class)
  public void modByZero() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    
    values.clear();
    values.put("a", 1);
    values.put("b", 0);
    evaluator.evaluate(new Tuple(values));
  }
  
  @Test
  public void modZeroByValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("mod(a,b)");
    Object result;
    
    values.clear();
    values.put("a", 0);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(0D, result);
  }
}
