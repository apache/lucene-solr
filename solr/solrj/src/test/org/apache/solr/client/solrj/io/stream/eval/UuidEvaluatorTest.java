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
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.eval.UuidEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class UuidEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public UuidEvaluatorTest() {
    super();
    
    factory = new StreamFactory()
      .withFunctionName("uuid", UuidEvaluator.class);
    values = new HashMap<String,Object>();
  }
    
  @Test
  public void testUuid() throws Exception{
    StreamEvaluator evaluator = factory.constructEvaluator("uuid()");
    Assert.assertTrue(evaluator.evaluate(null) instanceof String);
    String uuid = (String)evaluator.evaluate(null);
    assertEquals(uuid.split("-").length, 5);
    String uuid1 = (String)evaluator.evaluate(new Tuple(values));
    assertNotEquals(uuid, uuid1);
  }
}
