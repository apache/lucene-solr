package org.apache.solr.cloud;

/**
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.solr.core.SolrResourceLoader;

/**
 * ResourceLoader that works with ZooKeeper.
 *
 */
public class ZooKeeperSolrResourceLoader extends SolrResourceLoader {

  private String collection;

  private ZooKeeperReader zkReader;

  public ZooKeeperSolrResourceLoader(String instanceDir, String collection,
      ZooKeeperController zooKeeperController) {
    super(instanceDir);
    this.zkReader = zooKeeperController.getZkReader();
    this.collection = collection;
  }

  /**
   * <p>
   * This loader will first attempt to load resources from ZooKeeper, but if not found
   * will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files found in
   * the "lib/" directory in the specified instance directory.
   * <p>
   */
  public ZooKeeperSolrResourceLoader(String instanceDir, String collection, ClassLoader parent,
      Properties coreProperties, ZooKeeperController zooKeeperController) {
    super(instanceDir, parent, coreProperties);
    this.collection = collection;
    this.zkReader = zooKeeperController.getZkReader();
  }

  /**
   * Opens any resource by its name. By default, this will look in multiple
   * locations to load the resource: $configDir/$resource from ZooKeeper.
   * It will look for it in any jar
   * accessible through the class loader if it cannot be found in ZooKeeper. 
   * Override this method to customize loading resources.
   * 
   * @return the stream for the named resource
   */
  public InputStream openResource(String resource) {
    InputStream is = null;
    String file = getConfigDir() + "/" + resource; //nocommit: getConfigDir no longer makes sense here
    //nocommit:
    System.out.println("look for:" + file);
    try {
      if (zkReader.exists(file)) {
        byte[] bytes = zkReader.getFile(getConfigDir(), resource);
        return new ByteArrayInputStream(bytes);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error opening " + file, e);
    }
    try {
      // delegate to the class loader (looking into $INSTANCE_DIR/lib jars)
      is = classLoader.getResourceAsStream(resource);
    } catch (Exception e) {
      throw new RuntimeException("Error opening " + resource, e);
    }
    if (is == null) {
      throw new RuntimeException("Can't find resource '" + resource
          + "' in classpath or '" + getConfigDir() + "', cwd="
          + System.getProperty("user.dir"));
    }
    return is;
  }

  // nocommit: deal with code that uses this call to load the file itself (elevation?)
  public String getConfigDir() {
    return "/configs/" + collection;
  }
}
