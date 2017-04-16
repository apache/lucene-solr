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
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Provides a week stream evaluator
 */
public class TemporalEvaluatorWeek extends TemporalEvaluator {

  public static final String FUNCTION_NAME = "week";

  public TemporalEvaluatorWeek(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public String getFunction() {
    return FUNCTION_NAME;
  }

  @Override
  public Object evaluateDate(TemporalAccessor aDate) {
    return aDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
  }

}
