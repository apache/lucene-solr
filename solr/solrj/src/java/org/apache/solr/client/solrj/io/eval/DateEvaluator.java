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
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Provides numeric Date/Time stream evaluators
 */
public class DateEvaluator extends NumberEvaluator {

  public enum FUNCTION {year, month, day, dayofyear, dayofquarter, hour, minute, quarter, week, second, epoch};

  private FUNCTION function;
  private String fieldName;

  public DateEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    String functionName = expression.getFunctionName();

    try {
      this.function = FUNCTION.valueOf(functionName);
    } catch (IllegalArgumentException e) {
      throw new IOException(String.format(Locale.ROOT,"Invalid date expression %s - expecting one of %s",functionName, Arrays.toString(FUNCTION.values())));
    }

    fieldName = factory.getValueOperand(expression, 0);

    //Taken from Field evaluator
    if(fieldName != null && fieldName.startsWith("'") && fieldName.endsWith("'") && fieldName.length() > 1){
      fieldName = fieldName.substring(1, fieldName.length() - 1);
    }

    if(1 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one value but found %d",expression,subEvaluators.size()));
    }
  }

  //TODO: Support non-string, eg. java.util.date or instant

  @Override
  public Number evaluate(Tuple tuple) throws IOException {

    try {
      String dateStr = (String) tuple.get(fieldName);
      if (dateStr != null && !dateStr.isEmpty()) {
        Instant instant = Instant.parse(dateStr);
        if (function.equals(FUNCTION.epoch)) return instant.toEpochMilli();

        LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return evaluate(date);
      }
    } catch (ClassCastException | DateTimeParseException e) {
      throw new IOException(String.format(Locale.ROOT,"Invalid field %s - The field must be a string formatted in the ISO_INSTANT date format.",fieldName));
    }

    return null;
  }

  /**
   * Evaluate the date based on the specified function
   * @param date
   * @return the evaluated value
   */
  private Number evaluate(LocalDateTime date) {
    switch (function) {
      case year:
        return date.getYear();
      case month:
        return date.getMonthValue();
      case day:
        return date.getDayOfMonth();
      case dayofyear:
        return date.getDayOfYear();
      case hour:
        return date.getHour();
      case minute:
        return date.getMinute();
      case second:
        return date.getSecond();
      case dayofquarter:
        return date.get(IsoFields.DAY_OF_QUARTER);
      case quarter:
        return date.get(IsoFields.QUARTER_OF_YEAR);
      case week:
        return date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(function.toString()).withParameter(fieldName);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
        .withExpressionType(Explanation.ExpressionType.EVALUATOR)
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }

}
