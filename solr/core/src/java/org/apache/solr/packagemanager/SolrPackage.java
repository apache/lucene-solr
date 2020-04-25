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


import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

/**
 * Describes a package (along with all released versions) as it appears in a repository.
 */
public class SolrPackage implements Comparable<SolrPackage>, ReflectMapWriter {

  @JsonProperty("name")
  public String name;

  @JsonProperty("description")
  public String description;

  @JsonProperty("versions")
  public List<SolrPackageRelease> versions;

  @JsonProperty("repository")
  private String repository;

  @Override
  public String toString() {
    return jsonStr();
  }

  public static class SolrPackageRelease implements ReflectMapWriter {
    @JsonProperty("version")
    public String version;

    @JsonProperty("date")
    public Date date;

    @JsonProperty("artifacts")
    public List<Artifact> artifacts;

    @JsonProperty("manifest")
    public Manifest manifest;

    @Override
    public String toString() {
      return jsonStr();
    }
  }

  public static class Artifact implements ReflectMapWriter {
    @JsonProperty("url")
    public String url;

    @JsonProperty("sig")
    public String sig;
  }

  public static class Manifest implements ReflectMapWriter {
    @JsonProperty("version-constraint")
    public String versionConstraint;

    @JsonProperty("plugins")
    public List<Plugin> plugins;

    @JsonProperty("parameter-defaults")
    public Map<String, String> parameterDefaults;
  }

  public static class Plugin implements ReflectMapWriter {
    public String name;
    @JsonProperty("setup-command")
    public Command setupCommand;

    @JsonProperty("uninstall-command")
    public Command uninstallCommand;

    @JsonProperty("verify-command")
    public Command verifyCommand;

    @Override
    public String toString() {
      return jsonStr();
    }
  }

  @Override
  public int compareTo(SolrPackage o) {
    return name.compareTo(o.name);
  }

  public String getRepository() {
    return repository;
  }

  public void setRepository(String repository) {
    this.repository = repository;
  }

  public static class Command implements ReflectMapWriter {
    @JsonProperty("path")
    public String path;

    @JsonProperty("method")
    public String method;

    @JsonProperty("payload")
    public Map<String, Object> payload;

    @JsonProperty("condition")
    public String condition;

    @JsonProperty("expected")
    public String expected;
    
    @Override
      public String toString() {
        return jsonStr();
      }
  }
}

