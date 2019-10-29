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
import org.apache.solr.client.solrj.io.eval.FloorEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class FloorEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public FloorEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("floor", FloorEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void floorOneField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("floor(a)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1D, result);
    
    values.clear();
    values.put("a", 1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1D, result);
    
    values.clear();
    values.put("a", -1.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(-2D, result);
  }

  @Test(expected = IOException.class)
  public void floorNoField() throws Exception{
    factory.constructEvaluator("floor()");
  }
  
  @Test(expected = IOException.class)
  public void floorTwoFields() throws Exception{
    factory.constructEvaluator("floor(a,b)");
  }

  @Test//(expected = NumberFormatException.class)
  public void floorNoValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("floor(a)");
    
    values.clear();
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }
  @Test//(expected = NumberFormatException.class)
  public void floorNullValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("floor(a)");
    
    values.clear();
    values.put("a", null);
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }
}
