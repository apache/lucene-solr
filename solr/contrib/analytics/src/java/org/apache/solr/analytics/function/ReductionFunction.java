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
package org.apache.solr.analytics.function;

import java.util.function.UnaryOperator;

import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.value.AnalyticsValue;

/**
 * A function that reduces the values of a mapping expression, field or constant.
 */
public interface ReductionFunction extends AnalyticsValue {

  /**
   * Syncs the data collectors with shared versions across the entire Analytics Request
   * so that as little data as possible is sent across shards.
   *
   * @param sync a function that takes in a {@link ReductionDataCollector} and returns a shared version
   */
  void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync);
}

