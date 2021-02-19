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

package org.apache.solr.packagemanager;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.packagemanager.SolrPackage.Plugin;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Describes one instance of a package as it exists in Solr when installed.
 */
public class SolrPackageInstance implements ReflectMapWriter {
  @JsonProperty("name")
  final public String name;

  final public String description;

  @JsonProperty("version")
  final public String version;

  final public Manifest manifest;

  final public List<Plugin> plugins;

  final public Map<String, String> parameterDefaults;

  public List<String> files;

    @JsonIgnore
  private Object customData;
  
  @JsonIgnore
  public Object getCustomData() {
    return customData;
  }
  
  @JsonIgnore
  public void setCustomData(Object customData) {
    this.customData = customData;
  }
  
  public SolrPackageInstance(String id, String description, String version, Manifest manifest,
      List<Plugin> plugins, Map<String, String> params) {
    this.name = id;
    this.description = description;
    this.version = version;
    this.manifest = manifest;
    this.plugins = plugins;
    this.parameterDefaults = params;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    return name.equals(((SolrPackageInstance)obj).name) && version.equals(((SolrPackageInstance)obj).version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version);
  }

  @Override
  public String toString() {
    return jsonStr();
  }
}
