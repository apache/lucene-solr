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
import org.apache.solr.client.solrj.io.eval.AbsoluteValueEvaluator;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class AbsoluteValueEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public AbsoluteValueEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("abs", AbsoluteValueEvaluator.class)
      .withFunctionName("add", AddEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void absoluteValueOneField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("abs(a)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1D, result);
    
    values.clear();
    values.put("a", 1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.1D, result);
    
    values.clear();
    values.put("a", -1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.1D, result);
  }
  
  @Test
  public void absoluteValueFromContext() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("abs(a)");
    StreamContext context = new StreamContext();
    evaluator.setStreamContext(context);
    Object result;
    
    context.getLets().put("a", 1);
    result = evaluator.evaluate(new Tuple());
    Assert.assertEquals(1D, result);
    
    context.getLets().put("a", 1.1);
    result = evaluator.evaluate(new Tuple());
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.1D, result);
    
    context.getLets().put("a", -1.1);
    result = evaluator.evaluate(new Tuple());
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(1.1D, result);
    
    context.getLets().put("a", factory.constructEvaluator("add(4,-6,34,-56)"));
    result = evaluator.evaluate(new Tuple());
    Assert.assertEquals(24D, result);
  }

  @Test(expected = IOException.class)
  public void absNoField() throws Exception{
    factory.constructEvaluator("abs()");
  }
  
  @Test(expected = IOException.class)
  public void absTwoFields() throws Exception{
    factory.constructEvaluator("abs(a,b)");
  }

  @Test//(expected = NumberFormatException.class)
  public void absNoValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("abs(a)");
    
    values.clear();
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void absNullValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("abs(a)");
    
    values.clear();
    values.put("a", null);
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }
}
