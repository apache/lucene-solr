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

package org.apache.solr.common.util;

import java.util.function.BiConsumer;

/**
 * A simplified read-only Map like structure.
 * The objective is to provide implementations that are cheap and memory efficient to implement.
 * The keys are always {@link String} objects
 * No need to create all values up-front. Create objects on-demand
 */
public interface SimpleMap<T>  {

  /**get a value by key. If not present , null is returned
   *
   */
  T get(String key);

  /**get all the keys */
  Iterable<String> names();

  /**Navigate through all keys and values
   */
  void forEach(BiConsumer<String, T> consumer);
}
