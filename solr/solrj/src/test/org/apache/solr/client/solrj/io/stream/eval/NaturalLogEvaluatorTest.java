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
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.NaturalLogEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class NaturalLogEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;

  public NaturalLogEvaluatorTest() {
    super();

    factory = new StreamFactory()
        .withFunctionName("log", NaturalLogEvaluator.class).withFunctionName("add", AddEvaluator.class);
    values = new HashMap<String,Object>();
  }

  @Test
  public void logOneField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("log(a)");
    Object result;

    values.clear();
    values.put("a", 100);
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertTrue(result.equals(Math.log(100)));

  }

  @Test
  public void logNestedField() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("log(add(50,50))");
    Object result;

    values.clear();
    result = evaluator.evaluate(new Tuple(values));
    Assert.assertTrue(result instanceof Double);
    Assert.assertTrue(result.equals(Math.log(100)));

  }

  @Test(expected = IOException.class)
  public void logNoField() throws Exception{
    factory.constructEvaluator("log()");
  }

  @Test(expected = IOException.class)
  public void logTwoFields() throws Exception{
    factory.constructEvaluator("log(a,b)");
  }

  @Test
  public void logNoValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("log(a)");

    values.clear();
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }

  @Test
  public void logNullValue() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("log(a)");

    values.clear();
    values.put("a", null);
    Object result = evaluator.evaluate(new Tuple(values));
    assertNull(result);
  }
}
