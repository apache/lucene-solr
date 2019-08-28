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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ConversionEvaluator extends RecursiveNumericEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;

  private interface Converter {
    public double convert(double value);
  }
  enum LENGTH_CONSTANT {MILES, YARDS, FEET, INCHES, MILLIMETERS, CENTIMETERS, METERS, KILOMETERS};
  
  private LENGTH_CONSTANT from;
  private LENGTH_CONSTANT to;
  
  private Converter converter;
  
  public ConversionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(3 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 3 parameters but found %d", super.toExpression(constructingFactory), containedEvaluators.size()));
    }
    
    if(containedEvaluators.subList(0, 2).stream().anyMatch(item -> !(item instanceof RawValueEvaluator) && !(item instanceof FieldValueEvaluator))){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - first two parameters must be strings", super.toExpression(constructingFactory)));      
    }
    
    String fromString = containedEvaluators.get(0).toExpression(factory).toString().toUpperCase(Locale.ROOT);
    String toString = containedEvaluators.get(1).toExpression(factory).toString().toUpperCase(Locale.ROOT);
    
    try {
      from = LENGTH_CONSTANT.valueOf(fromString);
      to = LENGTH_CONSTANT.valueOf(toString);
      this.converter = constructConverter(from, to);
      
    } catch (IllegalArgumentException e) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - '%s' and '%s' are not both valid conversion types", super.toExpression(constructingFactory), fromString, toString));
    }
    
    // Remove evaluators 0 and 1 because we don't actually want those used
    containedEvaluators = containedEvaluators.subList(2, 3);
  }

  @Override
  public Object doWork(Object value) throws IOException{
    
    if(null == value){
      return null;
    }
    
    return converter.convert(((Number)value).doubleValue());
  }
  
  private Converter constructConverter(LENGTH_CONSTANT from, LENGTH_CONSTANT to) throws IOException {
    switch(from) {
      case INCHES:
        switch(to) {
          case MILLIMETERS:
            return (double value) -> value * 25.4;
          case CENTIMETERS:
            return (double value) -> value * 2.54;
          case METERS:
            return (double value) -> value * 0.0254;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case FEET:
        switch(to) {
          case METERS:
            return (double value) -> value * .30;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case YARDS:
        switch(to) {
          case METERS:
            return (double value) -> value * .91;
          case KILOMETERS:
            return (double value) -> value * 0.00091;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case MILES:
        switch(to) {
          case KILOMETERS:
            return (double value) -> value * 1.61;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case MILLIMETERS:
        switch (to) {
          case INCHES:
            return (double value) -> value * 0.039;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case CENTIMETERS:
        switch(to) {
          case INCHES:
            return (double value) -> value * 0.39;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case METERS:
        switch(to) {
          case FEET:
            return (double value) -> value * 3.28;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      case KILOMETERS:
        switch(to) {
          case MILES:
            return (double value) -> value * 0.62;
          case FEET:
            return (double value) -> value * 3280.8;
          default:
            throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
        }
      default:
        throw new EvaluatorException(String.format(Locale.ROOT,  "No conversion available from %s to %s", from, to));
    }
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    
    expression.addParameter(from.toString().toLowerCase(Locale.ROOT));
    expression.addParameter(to.toString().toLowerCase(Locale.ROOT));
    
    for(StreamEvaluator evaluator : containedEvaluators){
      expression.addParameter(evaluator.toExpression(factory));
    }
    return expression;
  }

}
