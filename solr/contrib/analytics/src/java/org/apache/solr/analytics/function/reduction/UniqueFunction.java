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
package org.apache.solr.analytics.function.reduction;

import java.util.function.UnaryOperator;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.function.ReductionFunction;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.function.reduction.data.UniqueCollector;
import org.apache.solr.analytics.function.reduction.data.UniqueCollector.UniqueDoubleCollector;
import org.apache.solr.analytics.function.reduction.data.UniqueCollector.UniqueFloatCollector;
import org.apache.solr.analytics.function.reduction.data.UniqueCollector.UniqueIntCollector;
import org.apache.solr.analytics.function.reduction.data.UniqueCollector.UniqueLongCollector;
import org.apache.solr.analytics.function.reduction.data.UniqueCollector.UniqueStringCollector;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.LongValue.AbstractLongValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A reduction function which returns the number of unique values of the given expression.
 */
public class UniqueFunction extends AbstractLongValue implements ReductionFunction {
  private UniqueCollector<?> collector;
  public static final String name = "unique";
  private final String exprStr;
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream param = params[0];
    UniqueCollector<?> collector;
    if (param instanceof IntValueStream) {
      collector = new UniqueIntCollector((IntValueStream)param);
    } else if (param instanceof LongValueStream) {
      collector = new UniqueLongCollector((LongValueStream)param);
    } else if (param instanceof FloatValueStream) {
      collector = new UniqueFloatCollector((FloatValueStream)param);
    } else if (param instanceof DoubleValueStream) {
      collector = new UniqueDoubleCollector((DoubleValueStream)param);
    } else if (param instanceof StringValueStream) {
      collector = new UniqueStringCollector((StringValueStream)param);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a comparable parameter.");
    }
    return new UniqueFunction(param, collector);
  });

  public UniqueFunction(AnalyticsValueStream param, UniqueCollector<?> collector) {
    this.collector = collector;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public long getLong() {
    return collector.count();
  }
  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (UniqueCollector<?>)sync.apply(collector);
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
    return ExpressionType.REDUCTION;
  }
}
