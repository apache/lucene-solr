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
import org.apache.solr.client.solrj.io.eval.ArcCosineEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import org.junit.Assert;

public class ArcCosineEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public ArcCosineEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("acos", ArcCosineEvaluator.class);
    values = new HashMap<String,Object>();
  }
  
  private void test(Double value) throws IOException{
    StreamEvaluator evaluator = factory.constructEvaluator("acos(a)");
    
    values.clear();
    values.put("a", value);
    Object result = evaluator.evaluate(new Tuple(values));
    
    if(null == value){
      Assert.assertNull(result);
    }
    else{
      Assert.assertTrue(result instanceof Double);
      Assert.assertEquals(Math.acos(value), result);
    }
  }
    
  @Test
  public void oneField() throws Exception{
    test(90D);
    test(45D);
    test(12.4D);
    test(-45D);
  }

  @Test(expected = IOException.class)
  public void noField() throws Exception{
    factory.constructEvaluator("acos()");
  }
  
  @Test(expected = IOException.class)
  public void twoFields() throws Exception{
    factory.constructEvaluator("acos(a,b)");
  }

  @Test//(expected = NumberFormatException.class)
  public void noValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("acos(a)");
    
    values.clear();
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }

  @Test//(expected = NumberFormatException.class)
  public void nullValue() throws Exception{
    test(null);
  }
}
