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
package org.apache.solr.cloud;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections.map.ReferenceMap;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrResourceNotFoundException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourceLoader that works with ZooKeeper.
 *
 */
public class ZkSolrResourceLoader extends SolrResourceLoader implements ResourceLoader {

  private final String configSetZkPath;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient zkClient;

  private static Map CONFIG_CACHE = new ReferenceMap(ReferenceMap.HARD, ReferenceMap.WEAK) {
    {
      purgeBeforeRead();
      purgeBeforeWrite();
    }
  };

  private static Map<String, ZkSolrResourceLoader.ZkByteArrayInputStream> SYNC_CONFIG_CACHE = Collections.synchronizedMap(CONFIG_CACHE);

  /**
   * <p>
   * This loader will first attempt to load resources from ZooKeeper, but if not found
   * will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files found in
   * the "lib/" directory in the specified instance directory.
   */
  public ZkSolrResourceLoader(Path instanceDir, String configSet, SolrResourceLoader parent,
                              ZkController zooKeeperController) {
    super(instanceDir, parent);
    this.zkClient = zooKeeperController.getZkClient();
    configSetZkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet;
  }

  /**
   * Opens any resource from zk by its name.
   * 
   * @return the stream for the named resource
   */
  @Override
  public InputStream openResource(String resource) throws IOException {

    String file = (".".equals(resource)) ? configSetZkPath : configSetZkPath + "/" + resource;
    if (log.isTraceEnabled()) log.trace("open resource {}", resource);

    try {

      boolean usedCached = false;

      ZkSolrResourceLoader.ZkByteArrayInputStream cached = SYNC_CONFIG_CACHE.get(file);
      if (cached != null) {
        Stat checkStat;
        try {
          checkStat = zkClient.exists(file, null);
        } catch (KeeperException | InterruptedException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        if (checkStat != null && checkStat.getVersion() <= cached.getStat().getVersion()) {
          return new ZkSolrResourceLoader.ZkByteArrayInputStream(cached.getBytes(), cached.getStat());
        }

      }

      Stat stat = new Stat();
      byte[] bytes = zkClient.getData(file, null, stat);
      if (bytes == null) {
        if (log.isDebugEnabled()) log.debug("resource not found {}", resource);
        throw new SolrResourceNotFoundException("Can't find resource '" + resource
                + "' in classpath or '" + configSetZkPath + "', cwd="
                + System.getProperty("user.dir"));
      }
      ZkByteArrayInputStream is = new ZkByteArrayInputStream(bytes, stat);
      SYNC_CONFIG_CACHE.put(file, (ZkSolrResourceLoader.ZkByteArrayInputStream) is);
      return is;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Interrupted while opening " + file, e);
    } catch (KeeperException.NoNodeException e) {
      throw new SolrResourceNotFoundException("Can't find resource '" + resource
              + "' in classpath or '" + configSetZkPath + "', cwd="
              + System.getProperty("user.dir"));
    } catch (KeeperException e) {
      log.error("zookeeper exception trying to open resource {}", resource);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error opening " + file, e);
    }
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  public static class ZkByteArrayInputStream extends ByteArrayInputStream{

    private final Stat stat;
    public ZkByteArrayInputStream(byte[] buf, Stat stat) {
      super(buf);
      this.stat = stat;
    }

    public Stat getStat(){
      return stat;
    }

    public byte[] getBytes() {
      return buf;
    }
  }

  public String getConfigSetZkPath() {
    return configSetZkPath;
  }
}
