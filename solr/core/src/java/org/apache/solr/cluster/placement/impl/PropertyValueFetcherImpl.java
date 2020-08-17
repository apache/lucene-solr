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

package org.apache.solr.cluster.placement.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyValue;
import org.apache.solr.cluster.placement.PropertyValueFetcher;

public class PropertyValueFetcherImpl implements PropertyValueFetcher {
  @Override
  public Map<PropertyKey, PropertyValue> fetchProperties(Set<PropertyKey> props) {
    // TODO implement! This implementation is as if no property can be fetched. Legal but useless.
    return new HashMap<>();
  }
}
