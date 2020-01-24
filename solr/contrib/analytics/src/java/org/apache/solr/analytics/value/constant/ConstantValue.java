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
package org.apache.solr.analytics.value.constant;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.analytics.ExpressionFactory.ConstantFunction;
import org.apache.solr.analytics.value.AnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * The parent class of all constant Analytics values.
 * <p>
 * Constant values can be specified in the following ways in analytics requests:
 * <ul>
 * <li> Constant booleans must match one of the following in any case: true, t, false, f
 * <li> Constant strings must be surrounded with "s or 's
 * <li> Constant numbers do not have to be surrounded with anything (floats are currently not supported)
 * <li> Constant dates must be formated in the ISO-8601 instant format
 * </ul>
 */
public abstract class ConstantValue implements AnalyticsValue {
  private static final Pattern truePattern = Pattern.compile("^true|t$", Pattern.CASE_INSENSITIVE);
  private static final Pattern falsePattern = Pattern.compile("^false|f$", Pattern.CASE_INSENSITIVE);

  public static final ConstantFunction creatorFunction = (param -> {
    param = param.trim();

    // Try to create a string
    if ((param.charAt(0)=='"' && param.charAt(param.length()-1)=='"')
        || (param.charAt(0)=='\'' && param.charAt(param.length()-1)=='\'')) {
      return new ConstantStringValue(param.substring(1, param.length()-1));
    }

    // Try to create a boolean
    Matcher m = truePattern.matcher(param);
    if (m.matches()) {
      return new ConstantBooleanValue(true);
    }
    m = falsePattern.matcher(param);
    if (m.matches()) {
      return new ConstantBooleanValue(false);
    }

    // Try to create a number
    try {
      long longTemp = Long.parseLong(param);
      if (longTemp == (int) longTemp) {
        return new ConstantIntValue((int) longTemp);
      } else {
        return new ConstantLongValue(longTemp);
      }
    } catch (NumberFormatException e1) {
      try {
        return new ConstantDoubleValue(Double.parseDouble(param));
      } catch (NumberFormatException e2) {}
    }

    // Try to create a date
    try {
      return new ConstantDateValue(Instant.parse(param).toEpochMilli());
    } catch (DateTimeParseException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The parameter "+param+" could not be cast to any constant.");
    }

  });

  @Override
  public AnalyticsValue convertToConstant() {
    return this;
  }

  static String createExpressionString(AnalyticsValueStream func,
                                       Object param) {
    return String.format(Locale.ROOT,"%s(%s)",
                         func.getName(),
                         param.toString());
  }
}
