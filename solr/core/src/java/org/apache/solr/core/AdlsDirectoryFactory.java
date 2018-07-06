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
package org.apache.solr.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.store.adls.AdlsDirectory;
import org.apache.solr.store.adls.AdlsLockFactory;
import org.apache.solr.store.adls.AdlsProvider;
import org.apache.solr.store.adls.FullAdlsProvider;
import org.apache.solr.store.blockcache.BlockCache;
import org.apache.solr.store.blockcache.BlockDirectory;
import org.apache.solr.store.blockcache.BlockDirectoryCache;
import org.apache.solr.store.blockcache.BufferStore;
import org.apache.solr.store.blockcache.Cache;
import org.apache.solr.store.blockcache.CustomBufferedIndexInput;
import org.apache.solr.store.blockcache.Metrics;
import org.apache.solr.util.HdfsUtil;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

public class AdlsDirectoryFactory extends CachingDirectoryFactory implements SolrCoreAware, SolrMetricProducer {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String BLOCKCACHE_SLAB_COUNT = "solr.adls.blockcache.slab.count";
  public static final String BLOCKCACHE_DIRECT_MEMORY_ALLOCATION = "solr.adls.blockcache.direct.memory.allocation";
  public static final String BLOCKCACHE_ENABLED = "solr.adls.blockcache.enabled";
  public static final String BLOCKCACHE_GLOBAL = "solr.adls.blockcache.global";
  public static final String BLOCKCACHE_READ_ENABLED = "solr.adls.blockcache.read.enabled";
  public static final String BLOCKCACHE_WRITE_ENABLED = "solr.adls.blockcache.write.enabled"; // currently buggy and disabled

  public static final String NRTCACHINGDIRECTORY_ENABLE = "solr.adls.nrtcachingdirectory.enable";
  public static final String NRTCACHINGDIRECTORY_MAXMERGESIZEMB = "solr.adls.nrtcachingdirectory.maxmergesizemb";
  public static final String NRTCACHINGDIRECTORY_MAXCACHEMB = "solr.adls.nrtcachingdirectory.maxcachedmb";
  public static final String NUMBEROFBLOCKSPERBANK = "solr.adls.blockcache.blocksperbank";

  public static final String KERBEROS_ENABLED = "solr.adls.security.kerberos.enabled";
  public static final String KERBEROS_KEYTAB = "solr.adls.security.kerberos.keytabfile";
  public static final String KERBEROS_PRINCIPAL = "solr.adls.security.kerberos.principal";

  public static final String ADLS_PROVIDER = "solr.adls.provider";


  public static final String ADLS_HOME = "solr.adls.home";
  public static final String CONFIG_DIRECTORY = "hdfs.confdir";

  public static final String CACHE_MERGES = "solr.adls.blockcache.cachemerges";
  public static final String CACHE_READONCE = "solr.adls.blockcache.cachereadonce";

  public static final String ADLS_OAUTH_CLIENT_ID = "dfs.adls.oauth2.client.id";
  public static final String ADLS_OAUTH_REFRESH_URL = "dfs.adls.oauth2.refresh.url";
  public static final String ADLS_OAUTH_CREDENTIAL = "dfs.adls.oauth2.credential";

  public static final String ADLS_READ_BUFFER_SIZE = "solr.adls.readbuffer.size.default";


  int adlsReadBufferSize;



  private SolrParams params;

  private String adlsDataDir;
  private String fqAdlsDataDir;


  private String confDir;

  private boolean cacheReadOnce;

  private boolean cacheMerges;

  private static BlockCache globalBlockCache;

  public static Metrics blockCacheMetrics;
  private static Boolean kerberosInit;

  private ADLStoreClient adlsClient1;

  private AdlsProvider provider;

  private final static class MetricsHolder {
    // [JCIP SE, Goetz, 16.6] Lazy initialization
    // Won't load until MetricsHolder is referenced
    public static final Metrics blockCacheMetrics = new Metrics();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public void init(NamedList args) {
    super.init(args);
    params = args.toSolrParams();
    this.fqAdlsDataDir = getConfig(ADLS_HOME, null);

    adlsReadBufferSize = getConfig(ADLS_READ_BUFFER_SIZE, CustomBufferedIndexInput.BUFFER_SIZE);

    Validate.isTrue(this.fqAdlsDataDir != null,"The data directory must specified");

    Validate.isTrue(
        this.fqAdlsDataDir.startsWith("adl://"),"The data directory must follow this format: adl://FQN/...");


    LOG.info(ADLS_HOME + "=" + this.adlsDataDir);

    String clientId = getConfig(ADLS_OAUTH_CLIENT_ID,"");
    String refreshUrl=getConfig(ADLS_OAUTH_REFRESH_URL,"");
    String crediential=getConfig(ADLS_OAUTH_CREDENTIAL,"");
    String adlsAcctFqdn= StringUtils.substringBetween(this.fqAdlsDataDir,"adl://","/");

    // if this is running on hadoop, we get the creds from the conf.  That way we can use the
    // hadoop credential store and not leave our keys where everyone can see them =-)
    Configuration conf = getConf(confDir = getConfig(CONFIG_DIRECTORY, null));
    if (conf != null){
      clientId = conf.get(ADLS_OAUTH_CLIENT_ID,"");
      refreshUrl=conf.get(ADLS_OAUTH_REFRESH_URL,"");
      crediential=conf.get(ADLS_OAUTH_CREDENTIAL,"");
    }

    Validate.isTrue(!StringUtils.isEmpty(clientId),"client id must be set");
    Validate.isTrue(!StringUtils.isEmpty(refreshUrl),"refresh url (AKA auth endpoint) must be set");
    Validate.isTrue(!StringUtils.isEmpty(crediential),"credential (AKA client secret) must be set");

    this.adlsDataDir=StringUtils.substringAfter(this.fqAdlsDataDir,"adl://"+adlsAcctFqdn);

    LOG.info("data directory {} connecting to ADLS acct {} putting data in {}",
        this.fqAdlsDataDir,adlsAcctFqdn,this.adlsDataDir);

    String aprovider = getConfig(ADLS_PROVIDER,"");
    if (StringUtils.isEmpty(aprovider)){
      AccessTokenProvider provider = new ClientCredsTokenProvider(refreshUrl,clientId,crediential);
      this.adlsClient1 = ADLStoreClient.createClient(adlsAcctFqdn,provider);
      this.provider  = new FullAdlsProvider(this.adlsClient1);
    } else {
      try {
        LOG.info("using alternative ADLS provider {}",aprovider);
        this.provider=(AdlsProvider)Class.forName(aprovider).newInstance();
      } catch (Exception e){
        throw new RuntimeException(e);
      }
    }

    cacheMerges = getConfig(CACHE_MERGES, false);
    cacheReadOnce = getConfig(CACHE_READONCE, false);

    boolean kerberosEnabled = getConfig(KERBEROS_ENABLED, false);
    LOG.info("Solr Kerberos Authentication "
        + (kerberosEnabled ? "enabled" : "disabled"));
    if (kerberosEnabled) {
      initKerberos();
    }
;
  }

  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    return AdlsLockFactory.INSTANCE;
  }


  public static final String fixBadPath(String path){
    return fixBadPath(path,true);
  }


    public static final String fixBadPath(String path,boolean truncatreFirstSlash){
    if (truncatreFirstSlash){
      if (path.startsWith("/")){
        path = path.substring(1);
      }
    }
    path = StringUtils.replace(path,"\\","/");
    return path;
  }

  @Override
  @SuppressWarnings("resource")
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    assert params != null : "init must be called before create";

    LOG.info("before fix {}",path);

    path = fixBadPath(path,false);

    LOG.info("creating directory factory for basedir {} path {}", adlsDataDir,path);

    if (blockCacheMetrics == null) {
      blockCacheMetrics = MetricsHolder.blockCacheMetrics;
    }

    boolean blockCacheEnabled = getConfig(BLOCKCACHE_ENABLED, true);
    boolean blockCacheGlobal = getConfig(BLOCKCACHE_GLOBAL, true);
    boolean blockCacheReadEnabled = getConfig(BLOCKCACHE_READ_ENABLED, true);

    if (!path.startsWith("/")){
      path = "/"+path;
    }

    String mypath=path;
    if (!path.startsWith(adlsDataDir)){
      mypath = adlsDataDir+path;
    }

    LOG.info("return ADLS directory factory for {}",mypath);

    Directory dir = new AdlsDirectory(mypath, provider,adlsReadBufferSize);
    if (blockCacheEnabled && dirContext != DirContext.META_DATA) {
      int numberOfBlocksPerBank = getConfig(NUMBEROFBLOCKSPERBANK, 16384);

      int blockSize = BlockDirectory.BLOCK_SIZE;

      int bankCount = getConfig(BLOCKCACHE_SLAB_COUNT, 1);

      boolean directAllocation = getConfig(BLOCKCACHE_DIRECT_MEMORY_ALLOCATION, true);

      int slabSize = numberOfBlocksPerBank * blockSize;
      LOG.info(
          "Number of slabs of block cache [{}] with direct memory allocation set to [{}]",
          bankCount, directAllocation);
      LOG.info(
          "Block cache target memory usage, slab size of [{}] will allocate [{}] slabs and use ~[{}] bytes",
          slabSize, bankCount,((long) bankCount * (long) slabSize));

      int bsBufferSize = params.getInt("solr.adls.blockcache.bufferstore.buffersize", blockSize);
      int bsBufferCount = params.getInt("solr.adls.blockcache.bufferstore.buffercount", 0); // this is actually total size

      BlockCache blockCache = getBlockDirectoryCache(numberOfBlocksPerBank,
          blockSize, bankCount, directAllocation, slabSize,
          bsBufferSize, bsBufferCount, blockCacheGlobal);

      Cache cache = new BlockDirectoryCache(blockCache, path, blockCacheMetrics, blockCacheGlobal);
      int readBufferSize = params.getInt("solr.adls.blockcache.read.buffersize", blockSize);
      dir = new AdlsDirectory(mypath, AdlsLockFactory.INSTANCE, provider, readBufferSize);
      dir = new BlockDirectory(mypath, dir, cache, null, blockCacheReadEnabled, false, cacheMerges, cacheReadOnce);
    }

    boolean nrtCachingDirectory = getConfig(NRTCACHINGDIRECTORY_ENABLE, true);
    if (nrtCachingDirectory) {
      double nrtCacheMaxMergeSizeMB = getConfig(NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 16);
      double nrtCacheMaxCacheMB = getConfig(NRTCACHINGDIRECTORY_MAXCACHEMB, 192);
      return new NRTCachingDirectory(dir, nrtCacheMaxMergeSizeMB, nrtCacheMaxCacheMB);
    }
    return dir;
  }

  boolean getConfig(String name, boolean defaultValue) {
    Boolean value = params.getBool(name);
    if (value == null) {
      String sysValue = System.getProperty(name);
      if (sysValue != null) {
        value = Boolean.valueOf(sysValue);
      }
    }
    return value == null ? defaultValue : value;
  }

  int getConfig(String name, int defaultValue) {
    Integer value = params.getInt(name);
    if (value == null) {
      String sysValue = System.getProperty(name);
      if (sysValue != null) {
        value = Integer.parseInt(sysValue);
      }
    }
    return value == null ? defaultValue : value;
  }

  String getConfig(String name, String defaultValue) {
    String value = params.get(name);
    if (value == null) {
      value = System.getProperty(name);
    }
    return value == null ? defaultValue : value;
  }

  private BlockCache getBlockDirectoryCache(int numberOfBlocksPerBank, int blockSize, int bankCount,
                                            boolean directAllocation, int slabSize, int bufferSize, int bufferCount, boolean staticBlockCache) {
    if (!staticBlockCache) {
      LOG.info("Creating new single instance HDFS BlockCache");
      return createBlockCache(numberOfBlocksPerBank, blockSize, bankCount, directAllocation, slabSize, bufferSize, bufferCount);
    }
    synchronized (HdfsDirectoryFactory.class) {

      if (globalBlockCache == null) {
        LOG.info("Creating new global HDFS BlockCache");
        globalBlockCache = createBlockCache(numberOfBlocksPerBank, blockSize, bankCount,
            directAllocation, slabSize, bufferSize, bufferCount);
      }
    }
    return globalBlockCache;
  }

  private BlockCache createBlockCache(int numberOfBlocksPerBank, int blockSize,
                                      int bankCount, boolean directAllocation, int slabSize, int bufferSize,
                                      int bufferCount) {
    BufferStore.initNewBuffer(bufferSize, bufferCount, blockCacheMetrics);
    long totalMemory = (long) bankCount * (long) numberOfBlocksPerBank
        * (long) blockSize;

    BlockCache blockCache;
    try {
      blockCache = new BlockCache(blockCacheMetrics, directAllocation, totalMemory, slabSize, blockSize);
    } catch (OutOfMemoryError e) {
      throw new RuntimeException(
          "The max direct memory is likely too low.  Either increase it (by adding -XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages to your containers startup args)"
              + " or disable direct allocation using solr.hdfs.blockcache.direct.memory.allocation=false in solrconfig.xml. If you are putting the block cache on the heap,"
              + " your java heap size might not be large enough."
              + " Failed allocating ~" + totalMemory / 1000000.0 + " MB.",
          e);
    }
    return blockCache;
  }

  @Override
  public boolean exists(String path) {
    try {
      return provider.checkExists(path);
    } catch (IOException e) {
      LOG.error("Error checking if ADLS path exists", e);
      throw new RuntimeException("Error checking if ADLS path exists", e);
    }
  }

  public Configuration getConf(String confDir) {
    if (confDir != null){
      File hdfsConf = new File(confDir);
      if (hdfsConf.exists()){
        Configuration conf = new Configuration();
        HdfsUtil.addHdfsResources(conf, confDir);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        return conf;
      }
    }
    return null;
  }

  @Override
  public boolean isAbsolute(String path) {
    return path.startsWith("adls:/");
  }

  @Override
  public boolean isPersistent() {
    return true;
  }

  @Override
  public boolean isSharedStorage() {
    return true;
  }

  @Override
  public boolean searchersReserveCommitPoints() {
    return true;
  }

  @Override
  public String getDataHome(CoreDescriptor cd) throws IOException {
    if (adlsDataDir == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "You must set the "
          + this.getClass().getSimpleName() + " param " + ADLS_HOME
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
        .trimLeadingAndTrailingSlashes(adlsDataDir)
        + "/"
        + path
        + "/"
        + cd.getDataDir()));
  }

  /**
   * @param directory to calculate size of
   * @return size in bytes
   * @throws IOException on low level IO error
   */
  @Override
  public long size(Directory directory) throws IOException {
    String adlsDirPath = getPath(directory);
    return size(adlsDirPath);
  }

  /**
   * @param path to calculate size of
   * @return size in bytes
   * @throws IOException on low level IO error
   */
  @Override
  public long size(String path) throws IOException {
    String pathz = fixBadPath(path,false);
    if (!pathz.startsWith("/")){
      pathz = "/"+pathz;
    }
    if (!pathz.startsWith(this.adlsDataDir)){
      pathz= this.adlsDataDir+pathz;
    }
    return provider.enumerateDirectory(pathz)
        .stream()
        .collect(Collectors.summingLong((entry) -> entry.length));
  }

  public String getConfDir() {
    return confDir;
  }

  private void initKerberos() {
    String keytabFile = getConfig(KERBEROS_KEYTAB, "").trim();
    if (keytabFile.length() == 0) {
      throw new IllegalArgumentException(KERBEROS_KEYTAB + " required because "
          + KERBEROS_ENABLED + " set to true");
    }
    String principal = getConfig(KERBEROS_PRINCIPAL, "");
    if (principal.length() == 0) {
      throw new IllegalArgumentException(KERBEROS_PRINCIPAL
          + " required because " + KERBEROS_ENABLED + " set to true");
    }
    synchronized (AdlsDirectoryFactory.class) {
      if (kerberosInit == null) {
        kerberosInit = new Boolean(true);
        final Configuration conf = getConf(getConfig(CONFIG_DIRECTORY, null));
        final String authVal = conf.get(HADOOP_SECURITY_AUTHENTICATION);
        final String kerberos = "kerberos";
        if (authVal != null && !authVal.equals(kerberos)) {
          throw new IllegalArgumentException(HADOOP_SECURITY_AUTHENTICATION
              + " set to: " + authVal + ", not kerberos, but attempting to "
              + " connect to HDFS via kerberos");
        }
        // let's avoid modifying the supplied configuration, just to be conservative
        final Configuration ugiConf = new Configuration(getConf(getConfig(CONFIG_DIRECTORY, null)));
        ugiConf.set(HADOOP_SECURITY_AUTHENTICATION, kerberos);
        UserGroupInformation.setConfiguration(ugiConf);
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

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    MetricsHolder.blockCacheMetrics.initializeMetrics(manager, registry, tag, scope);
  }

  @Override
  public void inform(SolrCore core) {
    setHost(core.getCoreContainer().getHostName());
  }

  @VisibleForTesting
  void setHost(String hostname) {
    //LocalityHolder.reporter.setHost(hostname);
  }

  @Override
  public void cleanupOldIndexDirectories(final String dataDir, final String currentIndexDir, boolean afterReload) {
    boolean pathExists = false;

    //String cleanPath = fixBadPath(this.adlsDataDir+"/"+fixBadPath(dataDir));



    String cleanDataDir = fixBadPath(dataDir,false);

    LOG.info("data dir {}");

    if (!cleanDataDir.startsWith("/")){
      cleanDataDir = "/"+cleanDataDir;
    }
    if (!cleanDataDir.startsWith(this.adlsDataDir)){
      cleanDataDir = this.adlsDataDir+"/"+cleanDataDir;
    }

    String cleanIndexDir = fixBadPath(currentIndexDir,false);

    if (!cleanIndexDir.startsWith("/")){
      cleanIndexDir = "/"+cleanIndexDir;
    }
    if (!cleanIndexDir.startsWith(this.adlsDataDir)){
      cleanIndexDir=this.adlsDataDir+"/"+cleanIndexDir;
    }



    final String myCurrentIndexDir=cleanIndexDir;

    if (cleanDataDir.endsWith("/")){
      cleanDataDir=cleanIndexDir.substring(0,cleanIndexDir.length()-1);
    }

    LOG.info("in data directory {} index directory {} clean up old files",cleanDataDir,cleanIndexDir);

    try {
      pathExists =provider.checkExists(cleanDataDir);
    } catch (IOException e) {
      LOG.error("Error checking if ADLS path "+cleanDataDir+" exists", e);
    }
    if (!pathExists) {
      LOG.warn("{} does not point to a valid data directory; skipping clean-up of old index directories.", cleanDataDir);
      return;
    }

    List<String> oldIndexPaths = null;
    try {
      oldIndexPaths=provider.enumerateDirectory(cleanDataDir).stream()
          .filter((path)->(!path.type.equals("DIRECTORY")))
          .filter((path)->(path.name.matches(INDEX_W_TIMESTAMP_REGEX)||(path.name.equals("index"))))
          .filter((path)->(!path.name.equals(myCurrentIndexDir)))
          .map((path)->path.fullName)
          .collect(Collectors.toList());
    } catch (FileNotFoundException fnfe) {
      // already deleted - ignore
      LOG.debug("Old index directory already deleted - skipping...", fnfe);
    } catch (IOException ioExc) {
      LOG.error("Error checking for old index directories to clean-up.", ioExc);
    }

    if (CollectionUtils.isEmpty(oldIndexPaths)){
      return;
    }
    Collections.sort(oldIndexPaths, Collections.reverseOrder());

    Set<String> livePaths = getLivePaths();

    int i = 0;
    if (afterReload) {
      LOG.info("Will not remove most recent old directory on reload {}", oldIndexPaths.get(0));
      i = 1;
    }
    LOG.info("Found {} old index directories to clean-up under {} afterReload={}", oldIndexPaths.size() - i, dataDir, afterReload);
    for (; i < oldIndexPaths.size(); i++) {
      String oldDirPath = oldIndexPaths.get(i);
      if (livePaths.contains(oldDirPath.toString())) {
        LOG.warn("Cannot delete directory {} because it is still being referenced in the cache.", oldDirPath);
      } else {
        try {
          if (provider.deleteRecursive(oldDirPath)) {
            LOG.info("Deleted old index directory {}", oldDirPath);
          } else {
            LOG.warn("Failed to delete old index directory {}", oldDirPath);
          }
        } catch (IOException e) {
          LOG.error("Failed to delete old index directory {} due to: {}", oldDirPath, e);
        }
      }
    }
  }

  // perform an atomic rename if possible
  public void renameWithOverwrite(Directory dir, String fileName, String toName) throws IOException {
    String adlsDirPath = getPath(dir);
    String file1 = adlsDirPath+"/"+fileName;
    String file2 = adlsDirPath+"/"+toName;

    LOG.debug("move {} to {} with overwrite",file1,file2);
    provider.rename(file1,file2,true);
  }

  @Override
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext) throws IOException {
    Directory baseFromDir = getBaseDir(fromDir);
    Directory baseToDir = getBaseDir(toDir);

    if (baseFromDir instanceof AdlsDirectory && baseToDir instanceof AdlsDirectory) {
      String dir1 = ((AdlsDirectory) baseFromDir).getAdlsDirPath();
      String dir2 = ((AdlsDirectory) baseToDir).getAdlsDirPath();
      String file1 = dir1+"/"+fileName;
      String file2 = dir2+"/"+fileName;

      LOG.debug("move {} to {} ",file1,file2);

      provider.rename(file1,file2,false);
      return;
    }

    super.move(fromDir, toDir, fileName, ioContext);
  }

  public AdlsProvider getProvider() {
    return provider;
  }
}
