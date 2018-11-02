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
package org.apache.solr.handler.dataimport;

import java.util.Map;

/**
 * <p>
 * Use this API to implement a custom transformer for any given entity
 * </p>
 * <p>
 * Implementations of this abstract class must provide a public no-args constructor.
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 *
 * @since solr 1.3
 */
public abstract class Transformer {
  /**
   * The input is a row of data and the output has to be a new row.
   *
   * @param context The current context
   * @param row     A row of data
   * @return The changed data. It must be a {@link Map}&lt;{@link String}, {@link Object}&gt; if it returns
   *         only one row or if there are multiple rows to be returned it must
   *         be a {@link java.util.List}&lt;{@link Map}&lt;{@link String}, {@link Object}&gt;&gt;
   */
  public abstract Object transformRow(Map<String, Object> row, Context context);
}
