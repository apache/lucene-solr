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

package org.apache.solr.common.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_DEF;

/**
 * Interact with solr cluster properties
 *
 * Note that all methods on this class make calls to ZK on every invocation.  For
 * read-only eventually-consistent uses, clients should instead call
 * {@link ZkStateReader#getClusterProperty(String, Object)}
 */
public class ClusterProperties {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String EXT_PROPRTTY_PREFIX = "ext.";
  
  private final SolrZkClient client;

  /**
   * Creates a ClusterProperties object using a provided SolrZkClient
   */
  public ClusterProperties(SolrZkClient client) {
    this.client = client;
  }

  /**
   * Read the value of a cluster property, returning a default if it is not set
   * @param key           the property name or the full path to the property.
   * @param defaultValue  the default value
   * @param <T>           the type of the property
   * @return the property value
   * @throws IOException if there is an error reading the value from the cluster
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(String key, T defaultValue) throws IOException {
    T value = (T) Utils.getObjectByPath(getClusterProperties(), false, key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Read the value of a cluster property, returning a default if it is not set
   *
   * @param key          the property name or the full path to the property as a list of parts.
   * @param defaultValue the default value
   * @param <T>          the type of the property
   * @return the property value
   * @throws IOException if there is an error reading the value from the cluster
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(List<String> key, T defaultValue) throws IOException {
    T value = (T) Utils.getObjectByPath(getClusterProperties(), false, key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Return the cluster properties
   * @throws IOException if there is an error reading properties from the cluster
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> getClusterProperties() throws IOException {
    try {
      Map<String, Object> properties = (Map<String, Object>) Utils.fromJSON(client.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true));
      return convertCollectionDefaultsToNestedFormat(properties);
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyMap();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading cluster property", SolrZkClient.checkInterrupted(e));
    }
  }

  public void setClusterProperties(Map<String, Object> properties) throws IOException, KeeperException, InterruptedException {
    client.atomicUpdate(ZkStateReader.CLUSTER_PROPS, zkData -> {
      if (zkData == null) return Utils.toJSON(convertCollectionDefaultsToNestedFormat(properties));
      @SuppressWarnings({"unchecked"})
      Map<String, Object> zkJson = (Map<String, Object>) Utils.fromJSON(zkData);
      zkJson = convertCollectionDefaultsToNestedFormat(zkJson);
      boolean modified = Utils.mergeJson(zkJson, convertCollectionDefaultsToNestedFormat(properties));
      return modified ? Utils.toJSON(zkJson) : null;
    });
  }

  /**
   * See SOLR-12827 for background. We auto convert any "collectionDefaults" keys to "defaults/collection" format.
   * This method will modify the given map and return the same object. Remove this method in Solr 9.
   *
   * @param properties the properties to be converted
   * @return the converted map
   */
  @SuppressWarnings({"unchecked"})
  static Map<String, Object> convertCollectionDefaultsToNestedFormat(Map<String, Object> properties) {
    if (properties.containsKey(COLLECTION_DEF)) {
      Map<String, Object> values = (Map<String, Object>) properties.remove(COLLECTION_DEF);
      if (values != null) {
        properties.putIfAbsent(CollectionAdminParams.DEFAULTS, new LinkedHashMap<>());
        Map<String, Object> defaults = (Map<String, Object>) properties.get(CollectionAdminParams.DEFAULTS);
        defaults.compute(CollectionAdminParams.COLLECTION, (k, v) -> {
          if (v == null) return values;
          else {
            ((Map) v).putAll(values);
            return v;
          }
        });
      } else {
        // explicitly set to null, so set null in the nested format as well
        properties.putIfAbsent(CollectionAdminParams.DEFAULTS, new LinkedHashMap<>());
        Map<String, Object> defaults = (Map<String, Object>) properties.get(CollectionAdminParams.DEFAULTS);
        defaults.put(CollectionAdminParams.COLLECTION, null);
      }
    }
    return properties;
  }

  /**
   * This method sets a cluster property.
   *
   * @param propertyName  The property name to be set.
   * @param propertyValue The value of the property, could also be a nested structure.
   * @throws IOException if there is an error writing data to the cluster
   */
  @SuppressWarnings("unchecked")
  public void setClusterProperty(String propertyName, Object propertyValue) throws IOException {

    validatePropertyName(propertyName);

    for (; ; ) {
      Stat s = new Stat();
      try {
        if (client.exists(ZkStateReader.CLUSTER_PROPS, true)) {
          @SuppressWarnings({"rawtypes"})
          Map properties = (Map) Utils.fromJSON(client.getData(ZkStateReader.CLUSTER_PROPS, null, s, true));
          if (propertyValue == null) {
            //Don't update ZK unless absolutely necessary.
            if (properties.get(propertyName) != null) {
              properties.remove(propertyName);
              client.setData(ZkStateReader.CLUSTER_PROPS, Utils.toJSON(properties), s.getVersion(), true);
            }
          } else {
            //Don't update ZK unless absolutely necessary.
            if (!propertyValue.equals(properties.get(propertyName))) {
              properties.put(propertyName, propertyValue);
              client.setData(ZkStateReader.CLUSTER_PROPS, Utils.toJSON(properties), s.getVersion(), true);
            }
          }
        } else {
          @SuppressWarnings({"rawtypes"})
          Map properties = new LinkedHashMap();
          properties.put(propertyName, propertyValue);
          client.create(ZkStateReader.CLUSTER_PROPS, Utils.toJSON(properties), CreateMode.PERSISTENT, true);
        }
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        //race condition
        continue;
      } catch (InterruptedException | KeeperException e) {
        throw new IOException("Error setting cluster property", SolrZkClient.checkInterrupted(e));
      }
      break;
    }
  }

  /**
   * The propertyName should be either: <br/>
   * 1. <code>ZkStateReader.KNOWN_CLUSTER_PROPS</code> that is used by solr itself.<br/>
   * 2. Custom property: it can be created by third-party extensions and should start with prefix <b>"ext."</b> and it's
   * recommended to also add prefix of plugin name or company name or package name to avoid conflict.
   * 
   * @param propertyName The property name to validate
   */
  private void validatePropertyName(String propertyName) {
    if (!ZkStateReader.KNOWN_CLUSTER_PROPS.contains(propertyName)
        && !propertyName.startsWith(EXT_PROPRTTY_PREFIX)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Not a known cluster property or starts with prefix "
          + EXT_PROPRTTY_PREFIX + ", propertyName: " + propertyName);
    }
  }
}
