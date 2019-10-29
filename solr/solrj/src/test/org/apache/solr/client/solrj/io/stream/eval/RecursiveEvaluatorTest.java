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
import org.apache.solr.client.solrj.io.eval.AndEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEvaluator;
import org.apache.solr.client.solrj.io.eval.IfThenElseEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEvaluator;
import org.apache.solr.client.solrj.io.eval.MultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.eval.SubtractEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class RecursiveEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public RecursiveEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("and", AndEvaluator.class)
      .withFunctionName("gt", GreaterThanEvaluator.class)
      .withFunctionName("lt", LessThanEvaluator.class)
      .withFunctionName("add", AddEvaluator.class)
      .withFunctionName("sub", SubtractEvaluator.class)
      .withFunctionName("mult", MultiplyEvaluator.class)
      .withFunctionName("if", IfThenElseEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void compoundTest() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("add(mult(a,b),if(gt(c,d),e,f),g)");
    Object result;
    
    values.clear();
    values.put("a", 10);
    values.put("b", -3);
    values.put("c", "foo");
    values.put("d", "bar");
    values.put("e", 9);
    values.put("f", 2);
    values.put("g", 5);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertEquals(-16D, result);
    
    values.clear();
    values.put("a", .1);
    values.put("b", -3);
    values.put("c", "foo");
    values.put("d", "bar");
    values.put("e", 9);
    values.put("f", 2);
    values.put("g", 5);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(13.7, result);
  }
}
