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

import java.io.Closeable;
import java.util.Properties;

/**
 * <p>
 * Provides data from a source with a given query.
 * </p>
 * <p>
 * Implementation of this abstract class must provide a default no-arg constructor
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public abstract class DataSource<T> implements Closeable {

  /**
   * Initializes the DataSource with the <code>Context</code> and
   * initialization properties.
   * <p>
   * This is invoked by the <code>DataImporter</code> after creating an
   * instance of this class.
   */
  public abstract void init(Context context, Properties initProps);

  /**
   * Get records for the given query.The return type depends on the
   * implementation .
   *
   * @param query The query string. It can be a SQL for JdbcDataSource or a URL
   *              for HttpDataSource or a file location for FileDataSource or a custom
   *              format for your own custom DataSource.
   * @return Depends on the implementation. For instance JdbcDataSource returns
   *         an Iterator&lt;Map &lt;String,Object&gt;&gt;
   */
  public abstract T getData(String query);

  /**
   * Cleans up resources of this DataSource after use.
   */
  public abstract void close();
}
