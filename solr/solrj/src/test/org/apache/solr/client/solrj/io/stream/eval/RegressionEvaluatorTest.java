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

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.RegressionEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import junit.framework.Assert;

public class RegressionEvaluatorTest extends SolrTestCase {

  StreamFactory factory;
  Map<String,Object> values;

  public RegressionEvaluatorTest() {
    super();
    factory = new StreamFactory().withFunctionName("regress", RegressionEvaluator.class);
    values = new HashMap<String,Object>();
  }

  @Test
  public void test() throws IOException {
    double[] l1 = new double[] {3.4, 4.5, 6.7};
    double[] l2 = new double[] {1.2, 3.2, 3};
    
    values.clear();
    values.put("l1", l1);
    values.put("l2", l2);

    Tuple result = (Tuple)factory.constructEvaluator("regress(l1,l2)").evaluate(new Tuple(values));
    
    SimpleRegression regression = new SimpleRegression();
    regression.addData(l1[0],l2[0]);
    regression.addData(l1[1],l2[1]);
    regression.addData(l1[2],l2[2]);
    
    Assert.assertEquals(result.getDouble("slope"), regression.getSlope());
    
  }

}
