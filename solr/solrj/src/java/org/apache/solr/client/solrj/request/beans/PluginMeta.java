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
  @JsonProperty(required = true)
  public String name;

  @JsonProperty(value = "class", required = true)
  public String klass;

  @JsonProperty
  public String version;

  @JsonProperty("path-prefix")
  public String pathPrefix;


  public PluginMeta copy() {
    PluginMeta result = new PluginMeta();
    result.name = name;
    result.klass = klass;
    result.version = version;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PluginMeta) {
      PluginMeta that = (PluginMeta) obj;
      return Objects.equals(this.name, that.name) &&
          Objects.equals(this.klass, that.klass) &&
          Objects.equals(this.version, that.version);
    }
    return false;
  }
  @Override
  public int hashCode() {
    return Objects.hash(name, version, klass);
  }
}
