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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Provides a epoch stream evaluator
 */
public class TemporalEvaluatorEpoch extends TemporalEvaluator {

  public static final String FUNCTION_NAME = "epoch";

  public TemporalEvaluatorEpoch(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public String getFunction() {
    return FUNCTION_NAME;
  }

  @Override
  public Object evaluateDate(TemporalAccessor aDate) throws IOException {
    if (aDate instanceof LocalDateTime) {
      return ((LocalDateTime)aDate).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
    throw new IOException(String.format(Locale.ROOT, "Unsupported function '%s' called on %s", FUNCTION_NAME, aDate.toString()));
  }

}
