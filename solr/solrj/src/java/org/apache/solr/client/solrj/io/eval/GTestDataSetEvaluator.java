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

package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.List;

import org.apache.commons.math3.stat.inference.GTest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.StreamParams;


public class GTestDataSetEvaluator extends RecursiveNumericListEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public GTestDataSetEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {

    @SuppressWarnings({"unchecked"})
    List<Number> listA = (List<Number>) value1;
    @SuppressWarnings({"unchecked"})
    List<Number> listB = (List<Number>) value2;

    long[] sampleA = new long[listA.size()];
    long[] sampleB = new long[listB.size()];

    for(int i=0; i<sampleA.length; i++) {
      sampleA[i] = listA.get(i).longValue();
    }

    for(int i=0; i<sampleB.length; i++) {
      sampleB[i] = listB.get(i).longValue();
    }

    GTest gTest = new GTest();
    double g = gTest.gDataSetsComparison(sampleA, sampleB);
    double p = gTest.gTestDataSetsComparison(sampleA, sampleB);

   Tuple tuple = new Tuple();
    tuple.put("G-statistic", g);
    tuple.put(StreamParams.P_VALUE, p);
    return tuple;
  }
}