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
package org.apache.solr.handler.dataimport;

import java.io.StringReader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *  A SolrCloud-friendly extension of {@link SimplePropertiesWriter}.  
 *  This implementation ignores the "directory" parameter, saving
 *  the properties file under /configs/[solrcloud collection name]/
 */
public class ZKPropertiesWriter extends SimplePropertiesWriter {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private String path;
  private SolrZkClient zkClient;
  
  @Override
  public void init(DataImporter dataImporter, Map<String, String> params) {
    super.init(dataImporter, params);    
    zkClient = dataImporter.getCore().getCoreContainer().getZkController().getZkClient();
  }
  
  @Override
  protected void findDirectory(DataImporter dataImporter, Map<String, String> params) {
    String collection = dataImporter.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
    path = "/configs/" + collection + "/" + filename;
  }
  
  @Override
  public boolean isWritable() {
    return true;
  }
  
  @Override
  public void persist(Map<String, Object> propObjs) {
    Properties existing = mapToProperties(readIndexerProperties());
    existing.putAll(mapToProperties(propObjs));
    StringWriter output = new StringWriter();
    try {
      existing.store(output, null);
      byte[] bytes = output.toString().getBytes(StandardCharsets.UTF_8);
      if (!zkClient.exists(path, false)) {
        try {
          zkClient.makePath(path, false);
        } catch (NodeExistsException e) {}
      }
      zkClient.setData(path, bytes, false);
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      log.warn("Could not persist properties to {} : {}", path, e.getClass(), e);
    }
  }
  
  @Override
  public Map<String, Object> readIndexerProperties() {
    Properties props = new Properties();
    try {
      byte[] data = zkClient.getData(path, null, null, true);
      if (data != null) {
        props.load(new StringReader(new String(data, StandardCharsets.UTF_8)));
      }
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      log.warn("Could not read DIH properties from {} : {}", path, e.getClass(), e);
    }
    return propertiesToMap(props);
  }
}
