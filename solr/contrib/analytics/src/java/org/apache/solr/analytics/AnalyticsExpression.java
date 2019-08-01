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
package org.apache.solr.analytics;

import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.value.AnalyticsValue;

/**
 * A wrapper for a top-level analytics expression.
 * The expression must have a name and be single valued.
 */
public class AnalyticsExpression {
  private final AnalyticsValue expression;
  private final String name;

  public AnalyticsExpression(String name, AnalyticsValue expression) {
    this.name = name;
    this.expression = expression;
  }

  public String getName() {
    return name;
  }

  public AnalyticsValue getExpression() {
    return expression;
  }

  /**
   * Get the current value of the expression.
   * This method can, and will, be called multiple times to return different values.
   * The value returned is based on the {@link ReductionDataCollection} given
   * to the {@link ReductionCollectionManager#setData} method.
   *
   * @return the current value of the expression
   */
  public Object toObject() {
    return expression.getObject();
  }

  /**
   * NOTE: Must be called after {@link #toObject()} is called, otherwise the value is not guaranteed to be correct.
   *
   * @return whether the current value of the expression exists.
   */
  public boolean exists() {
    return expression.exists();
  }
}