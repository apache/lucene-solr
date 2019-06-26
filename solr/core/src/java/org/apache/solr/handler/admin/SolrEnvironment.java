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

package org.apache.solr.handler.admin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;

/**
 * It is possible to define an environment code when starting Solr, through
 * -Dsolr.environment=prod|stage|test|dev or by setting the cluster property "environment".
 * This class checks if any of these are defined, and parses the string, which may also
 * contain custom overrides for environment name (label) and color to be shown in Admin UI
 */
public class SolrEnvironment {
  private String code = "unknown";
  private String label;
  private String color;
  private static Pattern pattern = Pattern.compile("^(prod|stage|test|dev)(,label=([\\w\\d+ _-]+))?(,color=([#\\w\\d]+))?");

  public String getCode() {
    return code;
  }

  public String getLabel() {
    return label == null ? null : label.replaceAll("\\+", " ");
  }

  public String getColor() {
    return color;
  }

  public boolean isDefined() {
    return !"unknown".equals(code);
  }

  /**
   * Parse an environment string of format &lt;prod|stage|test|dev&gt;
   * with an optional label and color as arguments
   * @param environmentString the raw string to parse
   * @return an instance of this object
   */
  public static SolrEnvironment parse(String environmentString) {
    SolrEnvironment env = new SolrEnvironment();
    if (environmentString == null || environmentString.equalsIgnoreCase("unknown")) {
      return env;
    }
    Matcher m = pattern.matcher(environmentString);
    if (!m.matches()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bad environment pattern: " + environmentString);
    }
    env.code = m.group(1);
    if (m.group(3) != null) {
      env.label = m.group(3);
    }
    if (m.group(5) != null) {
      env.color = m.group(5);
    }
    return env;
  }

  /**
   * Gets and parses the solr environment configuration string from either
   * System properties "solr.environment" or from Clusterprop "environment"
   * @param zkStateReader pass in the zkStateReader if in cloud mode
   * @return an instance of this class
   */
  public static SolrEnvironment getFromSyspropOrClusterprop(ZkStateReader zkStateReader) {
    String env = "unknown";
    if (System.getProperty("solr.environment") != null) {
      env = System.getProperty("solr.environment");
    } else if (zkStateReader != null) {
      env = zkStateReader.getClusterProperty("environment", "unknown");
    }
    return SolrEnvironment.parse(env);
  }
}
