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
package org.apache.solr.handler.sql;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.luke.FieldFlag;

import com.google.common.collect.ImmutableMap;

class SolrSchema extends AbstractSchema {
  final Properties properties;

  SolrSchema(Properties properties) {
    super();
    this.properties = properties;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    String zk = this.properties.getProperty("zk");
    try(CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zk), Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build()) {
      cloudSolrClient.connect();
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();

      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      for (String collection : clusterState.getCollectionsMap().keySet()) {
        builder.put(collection, new SolrTable(this, collection));
      }

      Aliases aliases = zkStateReader.getAliases();
      for (String alias : aliases.getCollectionAliasListMap().keySet()) {
        builder.put(alias, new SolrTable(this, alias));
      }

      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, LukeResponse.FieldInfo> getFieldInfo(String collection) {
    String zk = this.properties.getProperty("zk");
    try(CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zk), Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build()) {
      cloudSolrClient.connect();
      LukeRequest lukeRequest = new LukeRequest();
      lukeRequest.setNumTerms(0);
      LukeResponse lukeResponse = lukeRequest.process(cloudSolrClient, collection);
      return lukeResponse.getFieldInfo();
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  RelProtoDataType getRelDataType(String collection) {
    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
    Map<String, LukeResponse.FieldInfo> luceneFieldInfoMap = getFieldInfo(collection);

    for(Map.Entry<String, LukeResponse.FieldInfo> entry : luceneFieldInfoMap.entrySet()) {
      LukeResponse.FieldInfo luceneFieldInfo = entry.getValue();

      RelDataType type;
      switch (luceneFieldInfo.getType()) {
        case "string":
          type = typeFactory.createJavaType(String.class);
          break;
        case "tint":
        case "tlong":
        case "int":
        case "long":
        case "pint":
        case "plong":
          type = typeFactory.createJavaType(Long.class);
          break;
        case "tfloat":
        case "tdouble":
        case "float":
        case "double":
        case "pfloat":
        case "pdouble":
          type = typeFactory.createJavaType(Double.class);
          break;
        default:
          type = typeFactory.createJavaType(String.class);
      }

      EnumSet<FieldFlag> flags = luceneFieldInfo.parseFlags(luceneFieldInfo.getSchema());
      /*
      if(flags != null && flags.contains(FieldFlag.MULTI_VALUED)) {
        type = typeFactory.createArrayType(type, -1);
      }
      */

      fieldInfo.add(entry.getKey(), type).nullable(true);
    }
    fieldInfo.add("_query_",typeFactory.createJavaType(String.class));
    fieldInfo.add("score",typeFactory.createJavaType(Double.class));

    return RelDataTypeImpl.proto(fieldInfo.build());
  }
}
