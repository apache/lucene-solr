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

package org.apache.solr.managed;

import java.util.Arrays;

import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;

/**
 * Hierarchical component id.
 */
public class ManagedComponentId {

  public static final String SEPARATOR = ":";

  private final String type;
  private final String name;
  private final String[] path;
  private final String id;

  public ManagedComponentId(String type, Object component, String... path) {
    this(type, SolrMetricProducer.getUniqueMetricTag(component, null), path);
  }

  ManagedComponentId(String type, String name, String... path) {
    this.type = type;
    this.name = name;
    this.path = path;
    StringBuilder sb = new StringBuilder();
    sb.append(type);
    if (path != null) {
      for (String pathEl : path) {
        if (sb.length() > 0) {
          sb.append(SEPARATOR);
        }
        sb.append(pathEl);
      }
    }
    if (sb.length() > 0) {
      sb.append(SEPARATOR);
    }
    sb.append(name);
    id = sb.toString();
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String[] getPath() {
    return path;
  }

  public String toString() {
    return id;
  }

  public static ManagedComponentId of(String fullName) {
    if (fullName == null || fullName.isEmpty()) {
      return null;
    }
    String[] parts = fullName.split(SEPARATOR);
    if (parts.length < 2) {
      throw new RuntimeException("at least 2 parts (type and name) must be present: " + fullName);
    }
    if (parts.length > 2) {
      String type = parts[0];
      String name = parts[parts.length - 1];
      String[] path = Arrays.copyOfRange(parts, 1, parts.length - 1);
      return new ManagedComponentId(type, name, path);
    } else {
      return new ManagedComponentId(parts[0], parts[1]);
    }
  }
}