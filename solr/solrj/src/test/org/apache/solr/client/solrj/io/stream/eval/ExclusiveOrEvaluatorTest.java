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
import org.apache.solr.client.solrj.io.eval.ExclusiveOrEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class ExclusiveOrEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public ExclusiveOrEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("eor", ExclusiveOrEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void eorTwoBooleans() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eor(a,b)");
    Object result;
    
    values.clear();
    values.put("a", true);
    values.put("b", true);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", true);
    values.put("b", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", false);
    values.put("b", true);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", false);
    values.put("b", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
  }
  
  @Test
  public void eorWithSubAndsBooleans() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("eor(a,eor(b,c))");
    Object result;
    
    values.clear();
    values.put("a", true);
    values.put("b", true);
    values.put("c", true);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);

    values.clear();
    values.put("a", true);
    values.put("b", true);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
    
    values.clear();
    values.put("a", true);
    values.put("b", false);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", false);
    values.put("b", true);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(true, result);
    
    values.clear();
    values.put("a", false);
    values.put("b", false);
    values.put("c", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(false, result);
  }
}
