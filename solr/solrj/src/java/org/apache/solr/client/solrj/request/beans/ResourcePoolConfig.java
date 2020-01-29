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

package org.apache.solr.client.solrj.request.beans;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

/**
 *
 */
public class ResourcePoolConfig implements ReflectMapWriter {

  @JsonProperty(required = true)
  public String name;

  @JsonProperty(required = true)
  public String type;

  // note - these may be unmodifiable when constructed by ObjectMapper
  @JsonProperty
  public Map<String, Object> poolParams = new LinkedHashMap<>();

  @JsonProperty
  public Map<String, Object> poolLimits = new LinkedHashMap<>();

  @Override
  public String toString() {
    return "ResourcePoolConfig{" +
        "type='" + type + '\'' +
        ", name='" + name + '\'' +
        ", poolParams=" + poolParams +
        ", poolLimits=" + poolLimits +
        '}';
  }

  public boolean isValid() {
    return type != null && !type.isBlank() &&
        name != null && !name.isBlank() &&
        poolParams != null &&
        poolLimits != null;
  }
}
