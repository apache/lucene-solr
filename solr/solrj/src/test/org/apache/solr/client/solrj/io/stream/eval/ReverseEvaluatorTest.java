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
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.ReverseEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class ReverseEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public ReverseEvaluatorTest() {
    super();
    factory = new StreamFactory().withFunctionName("reverse", ReverseEvaluator.class);
    values = new HashMap<String,Object>();
  }

  @Test
  public void test() throws IOException {
    double[] l1 = new double[] {3.4, 6.7, 4.5};
    
    values.clear();
    values.put("l1", l1);

    @SuppressWarnings({"rawtypes"})
    List result = ((List<?>)factory.constructEvaluator("reverse(l1)").evaluate(new Tuple(values)));

    Assert.assertEquals(4.5, result.get(0));
    Assert.assertEquals(6.7, result.get(1));
    Assert.assertEquals(3.4, result.get(2));
  }
    
}
