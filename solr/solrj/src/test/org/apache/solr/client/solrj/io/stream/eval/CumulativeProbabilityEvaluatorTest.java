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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.CumulativeProbabilityEvaluator;
import org.apache.solr.client.solrj.io.eval.NormalDistributionEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class CumulativeProbabilityEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String, Object> values;
  
  public CumulativeProbabilityEvaluatorTest() {
    super();
    factory = new StreamFactory()
        .withFunctionName("prob", CumulativeProbabilityEvaluator.class)
        .withFunctionName("norm", NormalDistributionEvaluator.class);
    values = new HashMap<String,Object>();
  }

  @Test
  public void test() throws IOException {
    values.clear();
    values.put("l1", 3);
    values.put("l2", 7);

    NormalDistribution actual = new NormalDistribution(3,7);
    Assert.assertEquals(actual.cumulativeProbability(2), factory.constructEvaluator("prob(norm(l1,l2),2)").evaluate(new Tuple(values)));
  }
    
}
