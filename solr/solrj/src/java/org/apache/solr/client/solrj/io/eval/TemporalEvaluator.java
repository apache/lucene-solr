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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * A generic date evaluator for use with a TemporalAccessor
 */
public abstract class TemporalEvaluator extends ComplexEvaluator {

  private String field;

  public TemporalEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    if (1 != subEvaluators.size()) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting one value but found %d", expression, subEvaluators.size()));
    }
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {

    Instant instant = null;
    TemporalAccessor date = null;

    //First evaluate the parameter
    StreamEvaluator streamEvaluator = subEvaluators.get(0);
    Object tupleValue = streamEvaluator.evaluate(tuple);

    if (tupleValue == null) return null;

    if(field == null) {
      field = streamEvaluator.toExpression(constructingFactory).toString();
    }

    Map tupleContext = streamContext.getTupleContext();
    date = (LocalDateTime)tupleContext.get(field); // Check to see if the date has already been created for this field

    if(date == null) {
      if (tupleValue instanceof String) {
        instant = getInstant((String) tupleValue);
      } else if (tupleValue instanceof Long) {
        instant = Instant.ofEpochMilli((Long) tupleValue);
      } else if (tupleValue instanceof Instant) {
        instant = (Instant) tupleValue;
      } else if (tupleValue instanceof Date) {
        instant = ((Date) tupleValue).toInstant();
      } else if (tupleValue instanceof TemporalAccessor) {
        date = ((TemporalAccessor) tupleValue);
        tupleContext.put(field, date); // Cache the date in the TupleContext
      }
    }

    if (instant != null) {
      if (TemporalEvaluatorEpoch.FUNCTION_NAME.equals(getFunction())) return instant.toEpochMilli();
      date = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
      tupleContext.put(field, date); // Cache the date in the TupleContext
    }

    if (date != null) {
      try {
        return evaluateDate(date);
      } catch (UnsupportedTemporalTypeException utte) {
        throw new IOException(String.format(Locale.ROOT, "It is not possible to call '%s' function on %s", getFunction(), date.getClass().getName()));
      }
    }

    throw new IOException(String.format(Locale.ROOT, "Invalid parameter %s - The parameter must be a string formatted ISO_INSTANT or of type Long,Instant,Date,LocalDateTime or TemporalAccessor.", String.valueOf(tupleValue)));
  }

  public abstract Object evaluateDate(TemporalAccessor aDate) throws IOException;
  public abstract String getFunction();

  protected Instant getInstant(String dateStr) throws IOException {

    if (dateStr != null && !dateStr.isEmpty()) {
      try {
        return Instant.parse(dateStr);
      } catch (DateTimeParseException e) {
        throw new IOException(String.format(Locale.ROOT, "Invalid parameter %s - The String must be formatted in the ISO_INSTANT date format.", dateStr));
      }
    }
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(getFunction());

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
