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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.CoalesceEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class CoalesceEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public CoalesceEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("coalesce", CoalesceEvaluator.class);
    values = new HashMap<String,Object>();
  }
  /*
  @Test(expected = IOException.class)
  public void twoFieldsWithMissingField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("coalesce(a,b)");
    Object result;
    
    values.clear();
    values.put("a", null);
    values.put("b", 2);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(2L, result);
    
    values.clear();
    values.put("a", 1.1);
    values.put("b", null);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1.1D, result);
    
    values.clear();
    values.put("a", "foo");
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals("foo", result);

    values.clear();
    values.put("a", true);
    values.put("b", 2.1);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(true, result);
    

    values.clear();
    values.put("a", null);
    values.put("b", false);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(false, result);

    values.clear();
    values.put("a", null);
    values.put("b", null);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }


  @Test(expected = IOException.class)
  public void twoFieldsWithValues() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("coalesce(a,b)");
    Object result;
    
    values.clear();
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertNull(result);
  }
  */

  @Test
  public void manyFieldsWithSubcoalesces() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("coalesce(a,b,coalesce(c,d))");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", null);
    values.put("c", null);
    values.put("d", 4);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(1D, result);
  }
}
