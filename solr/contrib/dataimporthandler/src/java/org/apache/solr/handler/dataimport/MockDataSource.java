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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * A mock DataSource implementation which can be used for testing.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class MockDataSource extends
        DataSource<Iterator<Map<String, Object>>> {

  private static Map<String, Iterator<Map<String, Object>>> cache = new HashMap<>();

  public static void setIterator(String query,
                                 Iterator<Map<String, Object>> iter) {
    cache.put(query, iter);
  }

  public static void clearCache() {
    cache.clear();
  }

  @Override
  public void init(Context context, Properties initProps) {
  }

  @Override
  public Iterator<Map<String, Object>> getData(String query) {
    return cache.get(query);
  }

  @Override
  public void close() {
    cache.clear();

  }
}
