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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.AppendEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import org.junit.Assert;

public class AppendEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public AppendEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("append", AppendEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void multiField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("append(a,b,c)");
    Object result;
    
    values.clear();
    values.put("a", 1);
    values.put("b", Arrays.asList("foo","bar","baz"));
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof List);
    Assert.assertEquals(1D, ((List)result).get(0));
    Assert.assertEquals("foo", ((List)result).get(1));
    Assert.assertEquals("bar", ((List)result).get(2));
    Assert.assertEquals("baz", ((List)result).get(3));
    
  }
}
