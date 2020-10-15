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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.value.AnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValue.AbstractBooleanValue;

/**
 * A mapping function to test if a value.
 * <p>
 * Any {@link AnalyticsValueStream} can be passed in, and a {@link BooleanValue} will be returned representing whether a value exists.
 */
public class ExistsFunction {
  public static final String name = "exists";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 parameter.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof AnalyticsValue) {
      return new ValueExistsFunction((AnalyticsValue)param);
    }
    return new ValueStreamExistsFunction(param);
  });

  /**
   * Exists function that supports {@link AnalyticsValueStream}s.
   */
  static class ValueStreamExistsFunction extends AbstractBooleanValue {
    private final AnalyticsValueStream param;
    public static final String name = ExistsFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public ValueStreamExistsFunction(AnalyticsValueStream param) throws SolrException {
      this.param = param;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
    }

    private boolean exists;
    @Override
    public boolean getBoolean() {
      exists = false;
      param.streamObjects(val -> exists = true);
      return exists;
    }
    @Override
    public boolean exists() {
      return true;
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
   * Exists function that supports {@link AnalyticsValue}s.
   */
  static class ValueExistsFunction extends AbstractBooleanValue {
    private final AnalyticsValue param;
    public static final String name = ExistsFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public ValueExistsFunction(AnalyticsValue param) throws SolrException {
      this.param = param;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,param);
    }

    @Override
    public boolean getBoolean() {
      param.getObject();
      return param.exists();
    }
    @Override
    public boolean exists() {
      return true;
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

