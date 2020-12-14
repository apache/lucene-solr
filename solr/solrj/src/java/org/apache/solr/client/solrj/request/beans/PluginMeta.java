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

import java.util.Objects;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

/**
 * POJO for a plugin metadata used in container plugins
 */
public class PluginMeta implements ReflectMapWriter {
  /** Unique plugin name, required. */
  @JsonProperty(required = true)
  public String name;

  /** Plugin implementation class, required. */
  @JsonProperty(value = "class", required = true)
  public String klass;

  /** Plugin version. */
  @JsonProperty
  public String version;

  /** Plugin API path prefix, optional. */
  @JsonProperty("path-prefix")
  public String pathPrefix;

  /** Plugin configuration object, optional. */
  @JsonProperty
  public Object config;


  public PluginMeta copy() {
    PluginMeta result = new PluginMeta();
    result.name = name;
    result.klass = klass;
    result.version = version;
    result.config = config;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PluginMeta) {
      PluginMeta that = (PluginMeta) obj;
      return Objects.equals(this.name, that.name) &&
          Objects.equals(this.klass, that.klass) &&
          Objects.equals(this.version, that.version) &&
          Objects.equals(this.config, that.config);
    }
    return false;
  }
  @Override
  public int hashCode() {
    return Objects.hash(name, version, klass);
  }

  @Override
  public String toString() {
    return jsonStr();
  }
}
