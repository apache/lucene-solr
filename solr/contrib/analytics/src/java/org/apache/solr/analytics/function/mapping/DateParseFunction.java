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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.DateValue.AbstractDateValue;
import org.apache.solr.analytics.value.DateValueStream.AbstractDateValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A mapping function that converts long or string representations of dates to actual date objects.
 * <p>
 * The only parameter is the {@link LongValue}, {@link LongValueStream}, {@link DateValue}, or {@link DateValueStream} to convert. (Required)
 */
public class DateParseFunction {
  public static final String name = "date";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    if (params[0] instanceof LongValue) {
      return new LongToDateParseFunction((LongValue)params[0]);
    }
    else if (params[0] instanceof LongValueStream) {
      return new LongStreamToDateParseFunction((LongValueStream)params[0]);
    }
    else if (params[0] instanceof StringValue) {
      return new StringToDateParseFunction((StringValue)params[0]);
    }
    else if (params[0] instanceof StringValueStream) {
      return new StringStreamToDateParseFunction((StringValueStream)params[0]);
    }
    else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a string or long parameter. " +
          "Incorrect parameter: "+params[0].getExpressionStr());
    }
  });
}
class LongToDateParseFunction extends AbstractDateValue {
  private final LongValue param;
  public static final String name = DateParseFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongToDateParseFunction(LongValue param) throws SolrException {
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public long getLong() {
    return param.getLong();
  }
  @Override
  public boolean exists() {
    return param.exists();
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
class LongStreamToDateParseFunction extends AbstractDateValueStream {
  private final LongValueStream param;
  public static final String name = DateParseFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongStreamToDateParseFunction(LongValueStream param) throws SolrException {
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    param.streamLongs(cons);
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
class StringToDateParseFunction extends AbstractDateValue {
  private final StringValue param;
  public static final String name = DateParseFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringToDateParseFunction(StringValue param) throws SolrException {
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  private boolean exists = false;
  @Override
  public long getLong() {
    long value = 0;
    try {
      String paramStr = param.getString();
      exists = param.exists();
      if (exists) {
        value = Instant.parse(paramStr).toEpochMilli();
      }
    } catch (DateTimeParseException e) {
      exists = false;
    }
    return value;
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
class StringStreamToDateParseFunction extends AbstractDateValueStream {
  private final StringValueStream param;
  public static final String name = DateParseFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringStreamToDateParseFunction(StringValueStream param) throws SolrException {
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    param.streamStrings(value -> {
      try {
        cons.accept(Instant.parse(value).toEpochMilli());
      } catch (DateTimeParseException e) {}
    });
  }

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
