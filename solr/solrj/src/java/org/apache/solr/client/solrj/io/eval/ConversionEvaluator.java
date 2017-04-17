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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ConversionEvaluator extends ComplexEvaluator {

  enum LENGTH_CONSTANT {MILES, YARDS, FEET, INCHES, MILLIMETERS, CENTIMETERS, METERS, KILOMETERS};

  private LENGTH_CONSTANT from;
  private LENGTH_CONSTANT to;
  private Convert convert;

  public ConversionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    if (3 != subEvaluators.size()) {
      throw new EvaluatorException(new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting 3 value but found %d", expression, subEvaluators.size())));
    }

    try {
      from = LENGTH_CONSTANT.valueOf(subEvaluators.get(0).toExpression(factory).toString().toUpperCase(Locale.ROOT));
      to = LENGTH_CONSTANT.valueOf(subEvaluators.get(1).toExpression(factory).toString().toUpperCase(Locale.ROOT));
      this.convert = getConvert(from, to);
    } catch (IllegalArgumentException e) {
      throw new EvaluatorException(e);
    }
  }

  private String listParams() {
    StringBuffer buf = new StringBuffer();
    for(LENGTH_CONSTANT lc : LENGTH_CONSTANT.values()) {
      if(buf.length() > 0) {
        buf.append(", ");
      }
        buf.append(lc.toString());
    }
    return buf.toString();
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {

    StreamEvaluator streamEvaluator = subEvaluators.get(2);
    Object tupleValue = streamEvaluator.evaluate(tuple);

    if (tupleValue == null) return null;

    Number number = (Number)tupleValue;
    double d = number.doubleValue();
    return convert.convert(d);
  }

  private Convert getConvert(LENGTH_CONSTANT from, LENGTH_CONSTANT to) throws IOException {
    switch(from) {
      case INCHES:
        switch(to) {
          case MILLIMETERS:
            return (double d) -> d*25.4;
          case CENTIMETERS:
            return (double d) -> d*2.54;
          case METERS:
            return (double d) -> d*0.0254;
          default:
            throw new EvaluatorException("No conversion available from "+from+" to "+to);
        }
      case FEET:
        switch(to) {
          case METERS:
            return (double d) -> d * .30;
        }
      case YARDS:
        switch(to) {
          case METERS:
            return (double d) -> d * .91;
          case KILOMETERS:
            return (double d) -> d * 0.00091;
          default:
            throw new EvaluatorException("No conversion available from "+from+" to "+to);
        }
      case MILES:
        switch(to) {
          case KILOMETERS:
            return (double d) -> d * 1.61;
          default:
            throw new EvaluatorException("No conversion available from "+from+" to "+to);
        }
      case MILLIMETERS:
        switch (to) {
          case INCHES:
            return (double d) -> d * 0.039;
          default:
            throw new EvaluatorException("No conversion available from "+from+" to "+to);
        }
      case CENTIMETERS:
        switch(to) {
          case INCHES:
            return (double d) -> d * 0.39;
          default:
            throw new EvaluatorException("No conversion available from "+from+" to "+to);
        }
      case METERS:
        switch(to) {
          case FEET:
            return (double d) -> d * 3.28;
          default:
            throw new EvaluatorException("No conversion available from "+from+" to "+to);
        }
      case KILOMETERS:
        switch(to) {
          case MILES:
            return (double d) -> d * 0.62;
          case FEET:
            return (double d) -> d * 3280.8;
        }
      default:
        throw new EvaluatorException("No conversion available from "+from);
    }
  }

  private interface Convert {
    public double convert(double d);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    for (StreamEvaluator evaluator : subEvaluators) {
      expression.addParameter(evaluator.toExpression(factory));
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
        .withExpressionType(Explanation.ExpressionType.EVALUATOR)
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }
}