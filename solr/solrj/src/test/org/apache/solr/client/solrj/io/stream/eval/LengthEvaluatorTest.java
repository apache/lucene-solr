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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.LengthEvaluator;
import org.apache.solr.client.solrj.io.eval.SequenceEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class LengthEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public LengthEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("length", LengthEvaluator.class)
      .withFunctionName("sequence", SequenceEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void lengthField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("length(a)");
    Object result;
    
    values.clear();
    values.put("a", new ArrayList<Integer>(){{ add(1); add(2); add(4); }});
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Long);
    Assert.assertEquals(3L, result);
    
    values.clear();
    values.put("a", new ArrayList<String>(){{ add("a"); add("b"); }});
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Long);
    Assert.assertEquals(2L, result);
    
    values.clear();
    values.put("a", new ArrayList<String>(){{ }});
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Long);
    Assert.assertEquals(0L, result);    
  }

  @Test
  public void lengthEvaluator() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("length(sequence(3,4,10))");
    Object result;
    
    values.clear();
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Long);
    Assert.assertEquals(3L, result);    
  }
  
  @Test(expected = IOException.class)
  public void lengthValueNotCollection() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("length(a)");
    
    values.clear();
    values.put("a", "foo");
    evaluator.evaluate(new Tuple(values));    
  }
  
  @Test(expected = IOException.class)
  public void lengthNoField() throws Exception{
    factory.constructEvaluator("length()");
  }
  
  @Test(expected = IOException.class)
  public void lengthTwoFields() throws Exception{
    factory.constructEvaluator("length(a,b)");
  }
  
  @Test(expected = IOException.class)
  public void lengthNoValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("length(a)");
    
    values.clear();
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }
  @Test(expected = IOException.class)
  public void lengthNullValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("length(a)");
    
    values.clear();
    values.put("a", null);
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }
}
