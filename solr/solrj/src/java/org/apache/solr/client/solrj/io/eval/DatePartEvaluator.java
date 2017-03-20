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
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Provides numeric Date/Time stream evaluators
 */
public class DatePartEvaluator extends NumberEvaluator {

  public enum FUNCTION {year, month, day, dayofyear, dayofquarter, hour, minute, quarter, week, second, epoch}

  private final FUNCTION function;

  public DatePartEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    String functionName = expression.getFunctionName();

    try {
      this.function = FUNCTION.valueOf(functionName);
    } catch (IllegalArgumentException e) {
      throw new IOException(String.format(Locale.ROOT, "Invalid date expression %s - expecting one of %s", functionName, Arrays.toString(FUNCTION.values())));
    }

    if (1 != subEvaluators.size()) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting one value but found %d", expression, subEvaluators.size()));
    }
  }

  @Override
  public Number evaluate(Tuple tuple) throws IOException {

    Instant instant = null;
    TemporalAccessor date = null;

    //First evaluate the parameter
    StreamEvaluator streamEvaluator = subEvaluators.get(0);
    Object tupleValue = streamEvaluator.evaluate(tuple);

    if (tupleValue == null) return null;

    if (tupleValue instanceof String) {
      instant = getInstant((String) tupleValue);
    } else if (tupleValue instanceof Instant) {
      instant = (Instant) tupleValue;
    } else if (tupleValue instanceof Date) {
      instant = ((Date) tupleValue).toInstant();
    } else if (tupleValue instanceof TemporalAccessor) {
      date = ((TemporalAccessor) tupleValue);
    }

    if (instant != null) {
      if (function.equals(FUNCTION.epoch)) return instant.toEpochMilli();
      date = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    if (date != null) {
      return evaluate(date);
    }

    throw new IOException(String.format(Locale.ROOT, "Invalid parameter %s - The parameter must be a string formatted ISO_INSTANT or of type Instant,Date or LocalDateTime.", String.valueOf(tupleValue)));
  }

  private Instant getInstant(String dateStr) throws IOException {

    if (dateStr != null && !dateStr.isEmpty()) {
      try {
        return Instant.parse(dateStr);
      } catch (DateTimeParseException e) {
        throw new IOException(String.format(Locale.ROOT, "Invalid parameter %s - The String must be formatted in the ISO_INSTANT date format.", dateStr));
      }
    }
    return null;
  }

  /**
   * Evaluate the date based on the specified function
   *
   * @param date
   * @return the evaluated value
   */
  private Number evaluate(TemporalAccessor date) throws IOException {
    try {
      switch (function) {
        case year:
          return date.get(ChronoField.YEAR);
        case month:
          return date.get(ChronoField.MONTH_OF_YEAR);
        case day:
          return date.get(ChronoField.DAY_OF_MONTH);
        case dayofyear:
          return date.get(ChronoField.DAY_OF_YEAR);
        case hour:
          return date.get(ChronoField.HOUR_OF_DAY);
        case minute:
          return date.get(ChronoField.MINUTE_OF_HOUR);
        case second:
          return date.get(ChronoField.SECOND_OF_MINUTE);
        case dayofquarter:
          return date.get(IsoFields.DAY_OF_QUARTER);
        case quarter:
          return date.get(IsoFields.QUARTER_OF_YEAR);
        case week:
          return date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        case epoch:
          if (date instanceof LocalDateTime) {
            return ((LocalDateTime)date).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
          }
      }
    } catch (UnsupportedTemporalTypeException utte) {
      throw new IOException(String.format(Locale.ROOT, "It is not possible to call '%s' function on %s", function, date.getClass().getName()));
    }
    throw new IOException(String.format(Locale.ROOT, "Unsupported function '%s' called on %s", function, date.toString()));
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(function.toString());

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
