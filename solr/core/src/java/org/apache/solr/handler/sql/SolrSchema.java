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

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.schema.DateValueFieldType;
import org.apache.solr.schema.DoubleValueFieldType;
import org.apache.solr.schema.FloatValueFieldType;
import org.apache.solr.schema.IntValueFieldType;
import org.apache.solr.schema.LongValueFieldType;
import org.apache.solr.security.PKIAuthenticationPlugin;

class SolrSchema extends AbstractSchema implements Closeable {
  final Properties properties;
  final SolrClientCache solrClientCache;
  private volatile boolean isClosed = false;

  // collection schema gets requested 2x times during query execution,
  // so there's some benefit to caching it for the duration of a statement
  // every statement gets a new SolrSchema instance
  private Map<String, RelDataType> schemaCache = new ConcurrentHashMap<>();

  SolrSchema(Properties properties, SolrClientCache solrClientCache) {
    super();
    this.properties = properties;
    this.solrClientCache = solrClientCache;
  }

  public SolrClientCache getSolrClientCache() {
    return solrClientCache;
  }

  @Override
  public void close() {
    isClosed = true;
  }

  public boolean isClosed() {
    return isClosed;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    String zk = this.properties.getProperty("zk");
    CloudSolrClient cloudSolrClient = solrClientCache.getCloudSolrClient(zk);
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    Set<String> collections = clusterState.getCollectionsMap().keySet();
    for (String collection : collections) {
      builder.put(collection, new SolrTable(this, collection));
    }

    Aliases aliases = zkStateReader.getAliases();
    for (String alias : aliases.getCollectionAliasListMap().keySet()) {
      // don't create duplicate entries
      if (!collections.contains(alias)) {
        builder.put(alias, new SolrTable(this, alias));
      }
    }

    return builder.build();
  }

  private Map<String, LukeResponse.FieldInfo> getFieldInfo(final String collection) {
    final String zk = this.properties.getProperty("zk");
    PKIAuthenticationPlugin.withServerIdentity(true);
    try {
      LukeRequest lukeRequest = new LukeRequest();
      lukeRequest.setNumTerms(0);
      return lukeRequest.process(solrClientCache.getCloudSolrClient(zk), collection).getFieldInfo();
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      PKIAuthenticationPlugin.withServerIdentity(false);
    }
  }

  private LukeResponse getSchema(final String collection) {
    final String zk = this.properties.getProperty("zk");
    PKIAuthenticationPlugin.withServerIdentity(true);
    try {
      LukeRequest lukeRequest = new LukeRequest();
      lukeRequest.setShowSchema(true); // for empty fields and custom type info ...
      lukeRequest.setNumTerms(0);
      return lukeRequest.process(solrClientCache.getCloudSolrClient(zk), collection);
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      PKIAuthenticationPlugin.withServerIdentity(false);
    }
  }

  private boolean isStoredIndexedOrDocValues(final EnumSet<FieldFlag> flags) {
    // if a field is not stored but indexed, then we should still include it in the table schema so that users
    // can filter on it, they just won't be able to return it as a field
    return flags != null && (flags.contains(FieldFlag.DOC_VALUES) || flags.contains(FieldFlag.STORED) || flags.contains(FieldFlag.INDEXED));
  }

  private EnumSet<FieldFlag> getFieldFlags(final LukeResponse.FieldInfo luceneFieldInfo) {
    EnumSet<FieldFlag> flags = luceneFieldInfo.getSchemaFlags();
    if (flags == null) {
      String fieldSchema = luceneFieldInfo.getSchema();
      if (fieldSchema != null) {
        flags = LukeResponse.FieldInfo.parseFlags(fieldSchema);
      }
    }
    return flags;
  }

  RelProtoDataType getRelDataType(String collection) {
    return RelDataTypeImpl.proto(getRowSchema(collection));
  }

  RelDataType getRowSchema(String collection) {
    return schemaCache.computeIfAbsent(collection, this::buildRowSchema);
  }

  RelDataType buildRowSchema(String collection) {
    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    
    // Get fields that have data, including dynamic field instances
    Map<String, LukeResponse.FieldInfo> fieldsInUseMap = getFieldInfo(collection);

    LukeResponse schema = getSchema(collection);
    Map<String, LukeResponse.FieldInfo> storedFields = schema.getFieldInfo().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    // merge the actual fields in use returned by Luke with the declared fields in the schema that are empty
    Map<String, LukeResponse.FieldInfo> combinedFields = Stream.of(fieldsInUseMap, storedFields)
            .flatMap(map -> map.entrySet().stream())
            .filter(e -> isStoredIndexedOrDocValues(getFieldFlags(e.getValue()))) // Only want fields that are stored, indexed, or have docValues enabled
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, TreeMap::new));

    Map<String, Class<?>> javaClassForTypeMap = new HashMap<>(); // local cache for custom field types we've already resolved

    for (Map.Entry<String, LukeResponse.FieldInfo> entry : combinedFields.entrySet()) {
      LukeResponse.FieldInfo luceneFieldInfo = entry.getValue();

      String luceneFieldType = luceneFieldInfo.getType();
      // SOLR-13414: Luke can return a field definition with no type in rare situations
      if (luceneFieldType == null) {
        continue;
      }

      RelDataType type;

      // We have to pass multi-valued fields through Calcite as SQL Type ANY
      // Array doesn't work for aggregations! Calcite doesn't like GROUP BY on an ARRAY field
      // but Solr happily computes aggs on a multi-valued field, so we have a paradigm mis-match and
      // ANY is the best way to retain use of operators on multi-valued fields while still being able
      // to GROUP BY and project the multi-valued fields in results
      EnumSet<FieldFlag> flags = getFieldFlags(luceneFieldInfo);
      if (flags != null && flags.contains(FieldFlag.MULTI_VALUED)) {
        type = typeFactory.createSqlType(SqlTypeName.ANY);
      } else {
        switch (luceneFieldType) {
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
          case "pdate":
            type = typeFactory.createJavaType(Date.class);
            break;
          default:
            Class<?> javaClass = javaClassForTypeMap.get(luceneFieldType);
            if (javaClass == null) {
              javaClass = guessJavaClassForFieldType(schema.getFieldTypeInfo().get(luceneFieldType));
              javaClassForTypeMap.put(luceneFieldType, javaClass);
            }
            type = typeFactory.createJavaType(javaClass);
        }
      }
      fieldInfo.add(entry.getKey(), type).nullable(true);
    }
    fieldInfo.add("_query_", typeFactory.createJavaType(String.class));
    fieldInfo.add("score", typeFactory.createJavaType(Double.class));

    return fieldInfo.build();
  }

  private Class<?> guessJavaClassForFieldType(LukeResponse.FieldTypeInfo typeInfo) {
    Class<?> typeClass = null;
    if (typeInfo != null && !typeInfo.isTokenized() && typeInfo.getClassName() != null) {
      try {
        final Class<?> fieldTypeClass = getClass().getClassLoader().loadClass(typeInfo.getClassName());
        // a numeric type ... narrow down
        if (IntValueFieldType.class.isAssignableFrom(fieldTypeClass) || LongValueFieldType.class.isAssignableFrom(fieldTypeClass)) {
          typeClass = Long.class;
        } else if (FloatValueFieldType.class.isAssignableFrom(fieldTypeClass) || DoubleValueFieldType.class.isAssignableFrom(fieldTypeClass)) {
          typeClass = Double.class;
        } else if (DateValueFieldType.class.isAssignableFrom(fieldTypeClass)) {
          typeClass = Date.class;
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    // default to String if we could narrow it down by looking at the field type class
    return typeClass != null ? typeClass : String.class;
  }
}
