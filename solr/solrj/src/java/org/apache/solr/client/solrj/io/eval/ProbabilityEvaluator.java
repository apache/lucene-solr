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
import java.util.Locale;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ProbabilityEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public ProbabilityEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object... values) throws IOException{

    Object first = null;
    Object second = null;
    Object third = null;

    if(values.length == 2) {
      first = values[0];
      second = values[1];

      if (null == first) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the first value", toExpression(constructingFactory)));
      }
      if (null == second) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the second value", toExpression(constructingFactory)));
      }
      if (!(first instanceof IntegerDistribution)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a IntegerDistributionm for probability at a specific value.", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }
      if (!(second instanceof Number)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }

      IntegerDistribution d = (IntegerDistribution) first;
      Number predictOver = (Number) second;
      return d.probability(predictOver.intValue());

    } else if(values.length == 3) {
      first = values[0];
      second = values[1];
      third = values[2];

      if (!(first instanceof AbstractRealDistribution)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a RealDistribution for probability ranges", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }
      if (!(second instanceof Number)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }

      if (!(third instanceof Number)) {
        throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number", toExpression(constructingFactory), first.getClass().getSimpleName()));
      }

      AbstractRealDistribution realDistribution = (AbstractRealDistribution)first;
      Number start = (Number) second;
      Number end = (Number) third;
      return realDistribution.probability(start.doubleValue(), end.doubleValue());
    } else {
      throw new IOException("The probability function expects 2 or 3 parameters");
    }
  }
}
