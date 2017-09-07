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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;


public class ReplicaInfo implements MapWriter {
  final String name;
  String core, collection, shard;
  Replica.Type type;
  Map<String, Object> variables;

  public ReplicaInfo(String name, String coll, String shard, Replica.Type type, Map<String, Object> vals) {
    this.name = name;
    this.variables = vals;
    this.collection = coll;
    this.shard = shard;
    this.type = type;
    this.core = (String)vals.get("core");
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put(name, (MapWriter) ew1 -> {
      if (variables != null) {
        for (Map.Entry<String, Object> e : variables.entrySet()) {
          ew1.put(e.getKey(), e.getValue());
        }
      }
      if (type != null) ew1.put("type", type.toString());
    });
  }

  public String getCore() {
    return core;
  }

  public String getName() {
    return name;
  }

  public String getCollection() {
    return collection;
  }

  public String getShard() {
    return shard;
  }

  public Map<String, Object> getVariables() {
    return variables;
  }

  public Object getVariable(String name) {
    return variables != null ? variables.get(name) : null;
  }

  @Override
  public String toString() {
    return "ReplicaInfo{" +
        "name='" + name + '\'' +
        ", collection='" + collection + '\'' +
        ", shard='" + shard + '\'' +
        ", type=" + type +
        ", variables=" + variables +
        '}';
  }
}
