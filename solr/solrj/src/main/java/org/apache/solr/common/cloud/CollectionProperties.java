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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Interact with solr collection properties
 *
 * Note that all methods on this class make calls to ZK on every invocation.  For
 * read-only eventually-consistent uses, clients should instead call
 * {@link ZkStateReader#getCollectionProperties(String)}
 */
public class CollectionProperties {

  private final SolrZkClient client;

  /**
   * Creates a CollectionProperties object using a provided SolrZkClient
   */
  public CollectionProperties(SolrZkClient client) {
    this.client = client;
  }

  /**
   * Read the value of a collection property, returning a default if it is not set
   * @param key           the property name
   * @param defaultValue  the default value
   * @return the property value
   * @throws IOException if there is an error reading the value from zookeeper
   */
  public String getCollectionProperty(String collection, String key, String defaultValue) throws IOException {
    String value = getCollectionProperties(collection).get(key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Return the collection properties
   * @throws IOException if there is an error reading properties from zookeeper
   */
  @SuppressWarnings("unchecked")
  public Map<String, String> getCollectionProperties(String collection) throws IOException {
    try {
      return (Map<String, String>) Utils.fromJSON(client.getData(ZkStateReader.getCollectionPropsPath(collection), null, new Stat(), true));
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyMap();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading properties for collection " + collection, SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * This method sets a collection property.
   *
   * @param collection    The property name to be set.
   * @param propertyName  The property name to be set.
   * @param propertyValue The value of the property.
   * @throws IOException if there is an error writing data to zookeeper
   */
  @SuppressWarnings("unchecked")
  public void setCollectionProperty(String collection, String propertyName, String propertyValue) throws IOException {
    String znodePath = ZkStateReader.getCollectionPropsPath(collection);

    while (true) {
      Stat s = new Stat();
      try {
        if (client.exists(znodePath, true)) {
          Map<String, String> properties = (Map<String, String>) Utils.fromJSON(client.getData(znodePath, null, s, true));
          if (propertyValue == null) {
            if (properties.remove(propertyName) != null) { // Don't update ZK unless absolutely necessary.
              client.setData(znodePath, Utils.toJSON(properties), s.getVersion(), true);
            }
          } else {
            if (!propertyValue.equals(properties.put(propertyName, propertyValue))) { // Don't update ZK unless absolutely necessary.
              client.setData(znodePath, Utils.toJSON(properties), s.getVersion(), true);
            }
          }
        } else {
          Map<String, String> properties = new LinkedHashMap<>();
          properties.put(propertyName, propertyValue);
          client.create(znodePath, Utils.toJSON(properties), CreateMode.PERSISTENT, true);
        }
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        //race condition
        continue;
      } catch (InterruptedException | KeeperException e) {
        throw new IOException("Error setting property for collection " + collection, SolrZkClient.checkInterrupted(e));
      }
      break;
    }
  }
}
