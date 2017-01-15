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
package org.apache.solr.ltr.store.rest;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;

public class TestManagedFeatureStore extends LuceneTestCase {

  public static Map<String,Object> createMap(String name, String className, Map<String,Object> params) {
    final Map<String,Object> map = new HashMap<String,Object>();
    map.put(ManagedFeatureStore.NAME_KEY, name);
    map.put(ManagedFeatureStore.CLASS_KEY, className);
    if (params != null) {
      map.put(ManagedFeatureStore.PARAMS_KEY, params);
    }
    return map;
  }

}
