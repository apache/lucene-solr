package org.apache.solr.core;

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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.store.blockcache.BlockCache;
import org.apache.solr.store.blockcache.BlockDirectory;
import org.apache.solr.store.blockcache.BlockDirectoryCache;
import org.apache.solr.store.blockcache.BufferStore;
import org.apache.solr.store.blockcache.Cache;
import org.apache.solr.store.blockcache.Metrics;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.util.HdfsUtil;
import org.apache.solr.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsDirectoryFactory extends CachingDirectoryFactory {
  public static Logger LOG = LoggerFactory
      .getLogger(HdfsDirectoryFactory.class);
  
  public static final String BLOCKCACHE_SLAB_COUNT = "solr.hdfs.blockcache.slab.count";
  public static final String BLOCKCACHE_DIRECT_MEMORY_ALLOCATION = "solr.hdfs.blockcache.direct.memory.allocation";
  public static final String BLOCKCACHE_ENABLED = "solr.hdfs.blockcache.enabled";
  public static final String BLOCKCACHE_READ_ENABLED = "solr.hdfs.blockcache.read.enabled";
  public static final String BLOCKCACHE_WRITE_ENABLED = "solr.hdfs.blockcache.write.enabled";
  
  public static final String NRTCACHINGDIRECTORY_ENABLE = "solr.hdfs.nrtcachingdirectory.enable";
  public static final String NRTCACHINGDIRECTORY_MAXMERGESIZEMB = "solr.hdfs.nrtcachingdirectory.maxmergesizemb";
  public static final String NRTCACHINGDIRECTORY_MAXCACHEMB = "solr.hdfs.nrtcachingdirectory.maxcachedmb";
  public static final String NUMBEROFBLOCKSPERBANK = "solr.hdfs.blockcache.blocksperbank";
  
  public static final String KERBEROS_ENABLED = "solr.hdfs.security.kerberos.enabled";
  public static final String KERBEROS_KEYTAB = "solr.hdfs.security.kerberos.keytabfile";
  public static final String KERBEROS_PRINCIPAL = "solr.hdfs.security.kerberos.principal";
  
  public static final String HDFS_HOME = "solr.hdfs.home";
  
  public static final String CONFIG_DIRECTORY = "solr.hdfs.confdir";
  
  private SolrParams params;
  
  private String hdfsDataDir;
  
  private String confDir;
  
  public static Metrics metrics;
  private static Boolean kerberosInit;
  
  @Override
  public void init(NamedList args) {
    params = SolrParams.toSolrParams(args);
    this.hdfsDataDir = params.get(HDFS_HOME);
    if (this.hdfsDataDir != null && this.hdfsDataDir.length() == 0) {
      this.hdfsDataDir = null;
    }
    boolean kerberosEnabled = params.getBool(KERBEROS_ENABLED, false);
    LOG.info("Solr Kerberos Authentication "
        + (kerberosEnabled ? "enabled" : "disabled"));
    if (kerberosEnabled) {
      initKerberos();
    }
  }
  
  @Override
  protected Directory create(String path, DirContext dirContext)
      throws IOException {
    LOG.info("creating directory factory for path {}", path);
    Configuration conf = getConf();
    
    if (metrics == null) {
      metrics = new Metrics(conf);
    }
    
    boolean blockCacheEnabled = params.getBool(BLOCKCACHE_ENABLED, true);
    boolean blockCacheReadEnabled = params.getBool(BLOCKCACHE_READ_ENABLED,
        true);
    boolean blockCacheWriteEnabled = params.getBool(BLOCKCACHE_WRITE_ENABLED, true);
    Directory dir = null;
    
    if (blockCacheEnabled && dirContext != DirContext.META_DATA) {
      int numberOfBlocksPerBank = params.getInt(NUMBEROFBLOCKSPERBANK, 16384);
      
      int blockSize = BlockDirectory.BLOCK_SIZE;
      
      int bankCount = params.getInt(BLOCKCACHE_SLAB_COUNT, 1);
      
      boolean directAllocation = params.getBool(
          BLOCKCACHE_DIRECT_MEMORY_ALLOCATION, true);
      
      BlockCache blockCache;
      
      int slabSize = numberOfBlocksPerBank * blockSize;
      LOG.info(
          "Number of slabs of block cache [{}] with direct memory allocation set to [{}]",
          bankCount, directAllocation);
      LOG.info(
          "Block cache target memory usage, slab size of [{}] will allocate [{}] slabs and use ~[{}] bytes",
          new Object[] {slabSize, bankCount,
              ((long) bankCount * (long) slabSize)});
      
      int bufferSize = params.getInt("solr.hdfs.blockcache.bufferstore.buffersize", 128);
      int bufferCount = params.getInt("solr.hdfs.blockcache.bufferstore.buffercount", 128 * 128);
      
      BufferStore.initNewBuffer(bufferSize, bufferCount);
      long totalMemory = (long) bankCount * (long) numberOfBlocksPerBank
          * (long) blockSize;
      try {
        blockCache = new BlockCache(metrics, directAllocation, totalMemory,
            slabSize, blockSize);
      } catch (OutOfMemoryError e) {
        throw new RuntimeException(
            "The max direct memory is likely too low.  Either increase it (by adding -XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages to your containers startup args)"
                + " or disable direct allocation using solr.hdfs.blockcache.direct.memory.allocation=false in solrconfig.xml. If you are putting the block cache on the heap,"
                + " your java heap size might not be large enough."
                + " Failed allocating ~" + totalMemory / 1000000.0 + " MB.", e);
      }
      Cache cache = new BlockDirectoryCache(blockCache, metrics);
      HdfsDirectory hdfsDirectory = new HdfsDirectory(new Path(path), conf);
      dir = new BlockDirectory("solrcore", hdfsDirectory, cache, null,
          blockCacheReadEnabled, blockCacheWriteEnabled);
    } else {
      dir = new HdfsDirectory(new Path(path), conf);
    }
    
    boolean nrtCachingDirectory = params.getBool(NRTCACHINGDIRECTORY_ENABLE, true);
    if (nrtCachingDirectory) {
      double nrtCacheMaxMergeSizeMB = params.getInt(
          NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 16);
      double nrtCacheMaxCacheMB = params.getInt(NRTCACHINGDIRECTORY_MAXCACHEMB,
          192);
      
      return new NRTCachingDirectory(dir, nrtCacheMaxMergeSizeMB,
          nrtCacheMaxCacheMB);
    }
    return dir;
  }
  
  @Override
  public boolean exists(String path) {
    Path hdfsDirPath = new Path(path);
    Configuration conf = getConf();
    FileSystem fileSystem = null;
    try {
      fileSystem = FileSystem.newInstance(hdfsDirPath.toUri(), conf);
      return fileSystem.exists(hdfsDirPath);
    } catch (IOException e) {
      LOG.error("Error checking if hdfs path exists", e);
      throw new RuntimeException("Error checking if hdfs path exists", e);
    } finally {
      IOUtils.closeQuietly(fileSystem);
    }
  }
  
  private Configuration getConf() {
    Configuration conf = new Configuration();
    confDir = params.get(CONFIG_DIRECTORY, null);
    HdfsUtil.addHdfsResources(conf, confDir);
    return conf;
  }
  
  protected synchronized void removeDirectory(CacheValue cacheValue)
      throws IOException {
    Configuration conf = getConf();
    FileSystem fileSystem = null;
    try {
      fileSystem = FileSystem.newInstance(new URI(cacheValue.path), conf);
      boolean success = fileSystem.delete(new Path(cacheValue.path), true);
      if (!success) {
        throw new RuntimeException("Could not remove directory");
      }
    } catch (Exception e) {
      LOG.error("Could not remove directory", e);
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Could not remove directory", e);
    } finally {
      IOUtils.closeQuietly(fileSystem);
    }
  }
  
  @Override
  public boolean isAbsolute(String path) {
    return path.startsWith("hdfs:/");
  }
  
  @Override
  public boolean isPersistent() {
    return true;
  }
  
  @Override
  public boolean searchersReserveCommitPoints() {
    return true;
  }
  
  @Override
  public String getDataHome(CoreDescriptor cd) throws IOException {
    if (hdfsDataDir == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "You must set the "
          + this.getClass().getSimpleName() + " param " + HDFS_HOME
          + " for relative dataDir paths to work");
    }
    
    // by default, we go off the instance directory
    String path;
    if (cd.getCloudDescriptor() != null) {
      path = URLEncoder.encode(cd.getCloudDescriptor().getCollectionName(),
          "UTF-8")
          + "/"
          + URLEncoder.encode(cd.getCloudDescriptor().getCoreNodeName(),
              "UTF-8");
    } else {
      path = cd.getName();
    }
    
    return normalize(SolrResourceLoader.normalizeDir(ZkController
        .trimLeadingAndTrailingSlashes(hdfsDataDir)
        + "/"
        + path
        + "/"
        + cd.getDataDir()));
  }
  
  public String getConfDir() {
    return confDir;
  }
  
  private void initKerberos() {
    String keytabFile = params.get(KERBEROS_KEYTAB, "").trim();
    if (keytabFile.length() == 0) {
      throw new IllegalArgumentException(KERBEROS_KEYTAB + " required because "
          + KERBEROS_ENABLED + " set to true");
    }
    String principal = params.get(KERBEROS_PRINCIPAL, "");
    if (principal.length() == 0) {
      throw new IllegalArgumentException(KERBEROS_PRINCIPAL
          + " required because " + KERBEROS_ENABLED + " set to true");
    }
    synchronized (HdfsDirectoryFactory.class) {
      if (kerberosInit == null) {
        kerberosInit = new Boolean(true);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        LOG.info(
            "Attempting to acquire kerberos ticket with keytab: {}, principal: {} ",
            keytabFile, principal);
        try {
          UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        LOG.info("Got Kerberos ticket");
      }
    }
  }
}
