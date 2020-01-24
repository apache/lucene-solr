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
package org.apache.solr.analytics.function.field;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.analytics.value.AnalyticsValueStream;

/**
 * An analytics wrapper for Solr Fields.
 *
 * Currently only fields with Doc Values enabled can be used in Analytics queries.
 */
public abstract class AnalyticsField implements AnalyticsValueStream {
  protected static final int initialArrayLength = 20;

  protected final String fieldName;

  protected AnalyticsField(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public String getExpressionStr() {
    return fieldName;
  }

  @Override
  public String getName() {
    return fieldName;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.FIELD;
  }

  @Override
  public AnalyticsValueStream convertToConstant() {
    return this;
  }

  /**
   * Set the segment reader context
   *
   * @param context segment context
   * @throws IOException if an error occurs while loading the leaf reader
   */
  public abstract void doSetNextReader(LeafReaderContext context) throws IOException;

  /**
   * Collect the value(s) of the wrapped field for the given document, and store the value.
   *
   * @param doc ID of the document to collect
   * @throws IOException if an error occurs while reading the document.
   */
  public abstract void collect(int doc) throws IOException;

}
