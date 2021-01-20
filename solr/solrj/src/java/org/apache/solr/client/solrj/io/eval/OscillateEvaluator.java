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
import java.util.ArrayList;

import org.apache.commons.math3.analysis.function.HarmonicOscillator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class OscillateEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public OscillateEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object... objects) throws IOException{

    if(objects.length != 3) {
      throw new IOException("The oscillate function takes 3 arguments.");
    }

    double amp = ((Number)objects[0]).doubleValue();
    double om = ((Number)objects[1]).doubleValue();
    double phase = ((Number)objects[2]).doubleValue();


    HarmonicOscillator pf = new HarmonicOscillator(amp, om, phase);
    double[] x = new double[128];

    @SuppressWarnings({"rawtypes"})
    List list = new ArrayList();
    for(int i=0; i<128; i++) {
      double yvalue= pf.value(i);
      list.add(yvalue);
      x[i] = i;
    }

    VectorFunction func = new VectorFunction(pf, list);
    func.addToContext("x", x);
    return func;
  }
}
