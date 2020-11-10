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
package org.apache.solr.analytics.function.mapping;

import java.util.Date;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.DateValue.AbstractDateValue;
import org.apache.solr.analytics.value.DateValueStream.AbstractDateValueStream;
import org.apache.solr.analytics.value.constant.ConstantStringValue;
import org.apache.solr.analytics.value.constant.ConstantValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.util.DateMathParser;

/**
 * A mapping function that computes date math.
 * <p>
 * The first parameter is the {@link DateValue} or {@link DateValueStream} to compute date math on. (Required)
 * <br>
 * The trailing parameters must be constant date math strings (e.g. "+1DAY"). (At least 1 required)
 */
public class DateMathFunction {
  public static final String name = "date_math";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length < 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 2 paramaters, " + params.length + " found.");
    }
    StringBuilder mathParam = new StringBuilder();
    for (int i = 1; i < params.length; ++i) {
      if (params[i] instanceof StringValue && params[i] instanceof ConstantValue) {
        mathParam.append(((StringValue) params[i]).getString());
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires all math parameters to be a constant strings.");
      }
    }
    if (params[0] instanceof DateValue) {
      return new DateMathValueFunction((DateValue)params[0], new ConstantStringValue(mathParam.toString()));
    } else if (params[0] instanceof DateValueStream) {
      return new DateMathStreamFunction((DateValueStream)params[0], new ConstantStringValue(mathParam.toString()));
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a date as the first parameter.");
    }
  });

  /**
   * DateMath function that supports {@link DateValue}s.
   */
  static class DateMathValueFunction extends AbstractDateValue {
    private final DateValue dateParam;
    private final String mathParam;
    DateMathParser parser = new DateMathParser();
    public static final String name = DateMathFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateMathValueFunction(DateValue dateParam, ConstantStringValue mathParam) throws SolrException {
      this.dateParam = dateParam;
      this.mathParam = "NOW" + mathParam.getString();
      this.exprStr = AnalyticsValueStream.createExpressionString(name,dateParam,mathParam);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,dateParam);
    }

    private boolean exists = false;

    @Override
    public long getLong() {
      Date date = getDate();
      return (exists) ? date.getTime() : 0;
    }
    @Override
    public Date getDate() {
      Date date = dateParam.getDate();
      if (dateParam.exists()) {
        exists = true;
        return DateMathParser.parseMath(date,mathParam);
      } else {
        exists = false;
        return null;
      }
    }
    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return funcType;
    }
  }

  /**
   * DateMath function that supports {@link DateValueStream}s.
   */
  static class DateMathStreamFunction extends AbstractDateValueStream {
    private final DateValueStream dateParam;
    private final String mathParam;
    public static final String name = DateMathFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateMathStreamFunction(DateValueStream dateParam, ConstantStringValue mathParam) throws SolrException {
      this.dateParam = dateParam;
      this.mathParam = "NOW" + mathParam.getString();
      this.exprStr = AnalyticsValueStream.createExpressionString(name,dateParam,mathParam);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,dateParam);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      streamDates(value -> cons.accept(value.getTime()));
    }
    @Override
    public void streamDates(Consumer<Date> cons) {
      dateParam.streamDates(value -> cons.accept(DateMathParser.parseMath(value, mathParam)));
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return funcType;
    }
  }
}

