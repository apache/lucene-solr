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

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A bottom mapping function, returning the lowest value found.
 * <p>
 * Uses:
 * <ul>
 * <li>If a single comparable ValueStream is passed in, a Value (of the same type) representing the minimum of the values for each document is returned.
 * <li>If multiple comparable Values are passed in, a Value (of the same type) representing the minimum of all values is returned.
 * </ul>
 */
public class BottomFunction {
  public static final String name = "bottom";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires paramaters.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof DateValueStream) {
      if (params.length == 1) {
        return LambdaFunction.createDateLambdaFunction(name, (a,b) -> (a<b)? a:b, (DateValueStream)param);
      }
      DateValue[] castedParams = new DateValue[params.length];
      boolean tryNextType = false;
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof DateValue) {
          castedParams[i] = (DateValue) params[i];
        } else {
          tryNextType = true;
          break;
        }
      }
      if (!tryNextType) {
        return LambdaFunction.createDateLambdaFunction(name, (a,b) -> (a<b)? a:b, castedParams, false);
      }
    }
    if (param instanceof IntValueStream) {
      if (params.length == 1) {
        return LambdaFunction.createIntLambdaFunction(name, (a,b) -> (a<b)? a:b, (IntValueStream)param);
      }
      IntValue[] castedParams = new IntValue[params.length];
      boolean tryNextType = false;
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof IntValue) {
          castedParams[i] = (IntValue) params[i];
        } else {
          tryNextType = true;
          break;
        }
      }
      if (!tryNextType) {
        return LambdaFunction.createIntLambdaFunction(name, (a,b) -> (a<b)? a:b, castedParams, false);
      }
    }
    if (param instanceof LongValueStream) {
      if (params.length == 1) {
        return LambdaFunction.createLongLambdaFunction(name, (a,b) -> (a<b)? a:b, (LongValueStream)param);
      }
      LongValue[] castedParams = new LongValue[params.length];
      boolean tryNextType = false;
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof LongValue) {
          castedParams[i] = (LongValue) params[i];
        } else {
          tryNextType = true;
          break;
        }
      }
      if (!tryNextType) {
        return LambdaFunction.createLongLambdaFunction(name, (a,b) -> (a<b)? a:b, castedParams, false);
      }
    }
    if (param instanceof FloatValueStream) {
      if (params.length == 1) {
        return LambdaFunction.createFloatLambdaFunction(name, (a,b) -> (a<b)? a:b, (FloatValueStream)param);
      }
      FloatValue[] castedParams = new FloatValue[params.length];
      boolean tryNextType = false;
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof FloatValue) {
          castedParams[i] = (FloatValue) params[i];
        } else {
          tryNextType = true;
          break;
        }
      }
      if (!tryNextType) {
        return LambdaFunction.createFloatLambdaFunction(name, (a,b) -> (a<b)? a:b, castedParams, false);
      }
    }
    if (param instanceof DoubleValueStream) {
      if (params.length == 1) {
        return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> (a<b)? a:b, (DoubleValueStream)param);
      }
      DoubleValue[] castedParams = new DoubleValue[params.length];
      boolean tryNextType = false;
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof DoubleValue) {
          castedParams[i] = (DoubleValue) params[i];
        } else {
          tryNextType = true;
          break;
        }
      }
      if (!tryNextType) {
        return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> (a<b)? a:b, castedParams, false);
      }
    }
    if (param instanceof StringValueStream) {
      if (params.length == 1) {
        return LambdaFunction.createStringLambdaFunction(name, (a,b) -> (a.compareTo(b)<0)? a:b, (StringValueStream)param);
      }
      StringValue[] castedParams = new StringValue[params.length];
      boolean tryNextType = false;
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof StringValue) {
          castedParams[i] = (StringValue) params[i];
        } else {
          tryNextType = true;
          break;
        }
      }
      if (!tryNextType) {
        return LambdaFunction.createStringLambdaFunction(name, (a,b) -> (a.compareTo(b)<0)? a:b, castedParams, false);
      }
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a comparable parameter. " +
          "Incorrect parameter: "+params[0].getExpressionStr());
  });
}