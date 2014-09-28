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

package org.apache.solr.update;

import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * This config object encapsulates IndexWriter config params,
 * defined in the &lt;indexConfig&gt; section of solrconfig.xml
 */
public class SolrIndexConfig {
  public static final Logger log = LoggerFactory.getLogger(SolrIndexConfig.class);
  
  final String defaultMergePolicyClassName;
  public static final String DEFAULT_MERGE_SCHEDULER_CLASSNAME = ConcurrentMergeScheduler.class.getName();
  public final Version luceneVersion;
  
  /**
   * The explicit value of &lt;useCompoundFile&gt; specified on this index config
   * @deprecated use {@link #getUseCompoundFile}
   */
  @Deprecated
  public final boolean useCompoundFile;
  private boolean effectiveUseCompountFileSetting;

  public final int maxBufferedDocs;
  public final int maxMergeDocs;
  public final int maxIndexingThreads;
  public final int mergeFactor;

  public final double ramBufferSizeMB;

  public final int writeLockTimeout;
  public final String lockType;
  public final PluginInfo mergePolicyInfo;
  public final PluginInfo mergeSchedulerInfo;
  
  public final PluginInfo mergedSegmentWarmerInfo;
  
  public InfoStream infoStream = InfoStream.NO_OUTPUT;

  // Available lock types
  public final static String LOCK_TYPE_SIMPLE = "simple";
  public final static String LOCK_TYPE_NATIVE = "native";
  public final static String LOCK_TYPE_SINGLE = "single";
  public final static String LOCK_TYPE_NONE   = "none";

  public final boolean checkIntegrityAtMerge;

  /**
   * Internal constructor for setting defaults based on Lucene Version
   */
  @SuppressWarnings("deprecation")
  private SolrIndexConfig(SolrConfig solrConfig) {
    luceneVersion = solrConfig.luceneMatchVersion;
    useCompoundFile = effectiveUseCompountFileSetting = false;
    maxBufferedDocs = -1;
    maxMergeDocs = -1;
    maxIndexingThreads = IndexWriterConfig.DEFAULT_MAX_THREAD_STATES;
    mergeFactor = -1;
    ramBufferSizeMB = 100;
    writeLockTimeout = -1;
    lockType = LOCK_TYPE_NATIVE;
    mergePolicyInfo = null;
    mergeSchedulerInfo = null;
    defaultMergePolicyClassName = TieredMergePolicy.class.getName();
    mergedSegmentWarmerInfo = null;
    checkIntegrityAtMerge = false;
  }
  
  /**
   * Constructs a SolrIndexConfig which parses the Lucene related config params in solrconfig.xml
   * @param solrConfig the overall SolrConfig object
   * @param prefix the XPath prefix for which section to parse (mandatory)
   * @param def a SolrIndexConfig instance to pick default values from (optional)
   */
  @SuppressWarnings("deprecation")
  public SolrIndexConfig(SolrConfig solrConfig, String prefix, SolrIndexConfig def)  {
    if (prefix == null) {
      prefix = "indexConfig";
      log.debug("Defaulting to prefix \""+prefix+"\" for index configuration");
    }
    
    if (def == null) {
      def = new SolrIndexConfig(solrConfig);
    }

    // sanity check: this will throw an error for us if there is more then one
    // config section
    Object unused = solrConfig.getNode(prefix, false);

    luceneVersion = solrConfig.luceneMatchVersion;

    // Assert that end-of-life parameters or syntax is not in our config.
    // Warn for luceneMatchVersion's before LUCENE_3_6, fail fast above
    assertWarnOrFail("The <mergeScheduler>myclass</mergeScheduler> syntax is no longer supported in solrconfig.xml. Please use syntax <mergeScheduler class=\"myclass\"/> instead.",
        !((solrConfig.getNode(prefix+"/mergeScheduler",false) != null) && (solrConfig.get(prefix+"/mergeScheduler/@class",null) == null)),
        true);
    assertWarnOrFail("The <mergePolicy>myclass</mergePolicy> syntax is no longer supported in solrconfig.xml. Please use syntax <mergePolicy class=\"myclass\"/> instead.",
        !((solrConfig.getNode(prefix+"/mergePolicy",false) != null) && (solrConfig.get(prefix+"/mergePolicy/@class",null) == null)),
        true);
    assertWarnOrFail("The <luceneAutoCommit>true|false</luceneAutoCommit> parameter is no longer valid in solrconfig.xml.",
        solrConfig.get(prefix+"/luceneAutoCommit", null) == null,
        true);

    defaultMergePolicyClassName = def.defaultMergePolicyClassName;
    useCompoundFile=solrConfig.getBool(prefix+"/useCompoundFile", def.useCompoundFile);
    effectiveUseCompountFileSetting = useCompoundFile;
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    maxMergeDocs=solrConfig.getInt(prefix+"/maxMergeDocs",def.maxMergeDocs);
    maxIndexingThreads=solrConfig.getInt(prefix+"/maxIndexingThreads",def.maxIndexingThreads);
    mergeFactor=solrConfig.getInt(prefix+"/mergeFactor",def.mergeFactor);
    ramBufferSizeMB = solrConfig.getDouble(prefix+"/ramBufferSizeMB", def.ramBufferSizeMB);

    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);

    mergeSchedulerInfo = getPluginInfo(prefix + "/mergeScheduler", solrConfig, def.mergeSchedulerInfo);
    mergePolicyInfo = getPluginInfo(prefix + "/mergePolicy", solrConfig, def.mergePolicyInfo);
    
    String val = solrConfig.get(prefix + "/termIndexInterval", null);
    if (val != null) {
      throw new IllegalArgumentException("Illegal parameter 'termIndexInterval'");
    }

    boolean infoStreamEnabled = solrConfig.getBool(prefix + "/infoStream", false);
    if(infoStreamEnabled) {
      String infoStreamFile = solrConfig.get(prefix + "/infoStream/@file", null);
      if (infoStreamFile == null) {
        log.info("IndexWriter infoStream solr logging is enabled");
        infoStream = new LoggingInfoStream();
      } else {
        throw new IllegalArgumentException("Remove @file from <infoStream> to output messages to solr's logfile");
      }
    }
    mergedSegmentWarmerInfo = getPluginInfo(prefix + "/mergedSegmentWarmer", solrConfig, def.mergedSegmentWarmerInfo);
    if (mergedSegmentWarmerInfo != null && solrConfig.nrtMode == false) {
      throw new IllegalArgumentException("Supplying a mergedSegmentWarmer will do nothing since nrtMode is false");
    }

    checkIntegrityAtMerge = solrConfig.getBool(prefix + "/checkIntegrityAtMerge", def.checkIntegrityAtMerge);
  }

  /*
   * Assert that assertCondition is true.
   * If not, prints reason as log warning.
   * If failCondition is true, then throw exception instead of warning 
   */
  private void assertWarnOrFail(String reason, boolean assertCondition, boolean failCondition) {
    if(assertCondition) {
      return;
    } else if(failCondition) {
      throw new SolrException(ErrorCode.FORBIDDEN, reason);
    } else {
      log.warn(reason);
    }
  }

  private PluginInfo getPluginInfo(String path, SolrConfig solrConfig, PluginInfo def)  {
    List<PluginInfo> l = solrConfig.readPluginInfos(path, false, true);
    return l.isEmpty() ? def : l.get(0);
  }

  public IndexWriterConfig toIndexWriterConfig(IndexSchema schema) {
    // so that we can update the analyzer on core reload, we pass null
    // for the default analyzer, and explicitly pass an analyzer on 
    // appropriate calls to IndexWriter
    
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    if (maxBufferedDocs != -1)
      iwc.setMaxBufferedDocs(maxBufferedDocs);

    if (ramBufferSizeMB != -1)
      iwc.setRAMBufferSizeMB(ramBufferSizeMB);

    if (writeLockTimeout != -1)
      iwc.setWriteLockTimeout(writeLockTimeout);

    iwc.setSimilarity(schema.getSimilarity());
    iwc.setMergePolicy(buildMergePolicy(schema));
    iwc.setMergeScheduler(buildMergeScheduler(schema));
    iwc.setInfoStream(infoStream);

    // do this after buildMergePolicy since the backcompat logic 
    // there may modify the effective useCompoundFile
    iwc.setUseCompoundFile(getUseCompoundFile());

    if (maxIndexingThreads != -1) {
      iwc.setMaxThreadStates(maxIndexingThreads);
    }
    
    if (mergedSegmentWarmerInfo != null) {
      // TODO: add infostream -> normal logging system (there is an issue somewhere)
      IndexReaderWarmer warmer = schema.getResourceLoader().newInstance(mergedSegmentWarmerInfo.className, 
                                                                        IndexReaderWarmer.class,
                                                                        null,
                                                                        new Class[] { InfoStream.class },
                                                                        new Object[] { iwc.getInfoStream() });
      iwc.setMergedSegmentWarmer(warmer);
    }

    return iwc;
  }

  /**
   * Builds a MergePolicy, may also modify the value returned by
   * getUseCompoundFile() for use by the IndexWriterConfig if 
   * "useCompoundFile" is specified as an init arg for 
   * an out of the box MergePolicy that no longer supports it
   *
   * @see #fixUseCFMergePolicyInitArg
   * @see #getUseCompoundFile
   */
  private MergePolicy buildMergePolicy(IndexSchema schema) {
    String mpClassName = mergePolicyInfo == null ? defaultMergePolicyClassName : mergePolicyInfo.className;

    MergePolicy policy = schema.getResourceLoader().newInstance(mpClassName, MergePolicy.class);

    if (policy instanceof LogMergePolicy) {
      LogMergePolicy logMergePolicy = (LogMergePolicy) policy;
      fixUseCFMergePolicyInitArg(LogMergePolicy.class);

      if (maxMergeDocs != -1)
        logMergePolicy.setMaxMergeDocs(maxMergeDocs);

      logMergePolicy.setNoCFSRatio(getUseCompoundFile() ? 1.0 : 0.0);

      if (mergeFactor != -1)
        logMergePolicy.setMergeFactor(mergeFactor);


    } else if (policy instanceof TieredMergePolicy) {
      TieredMergePolicy tieredMergePolicy = (TieredMergePolicy) policy;
      fixUseCFMergePolicyInitArg(TieredMergePolicy.class);
      
      tieredMergePolicy.setNoCFSRatio(getUseCompoundFile() ? 1.0 : 0.0);
      
      if (mergeFactor != -1) {
        tieredMergePolicy.setMaxMergeAtOnce(mergeFactor);
        tieredMergePolicy.setSegmentsPerTier(mergeFactor);
      }


    } else if (mergeFactor != -1) {
      log.warn("Use of <mergeFactor> cannot be configured if merge policy is not an instance of LogMergePolicy or TieredMergePolicy. The configured policy's defaults will be used.");
    }

    if (mergePolicyInfo != null)
      SolrPluginUtils.invokeSetters(policy, mergePolicyInfo.initArgs);

    return policy;
  }

  private MergeScheduler buildMergeScheduler(IndexSchema schema) {
    String msClassName = mergeSchedulerInfo == null ? SolrIndexConfig.DEFAULT_MERGE_SCHEDULER_CLASSNAME : mergeSchedulerInfo.className;
    MergeScheduler scheduler = schema.getResourceLoader().newInstance(msClassName, MergeScheduler.class);

    if (mergeSchedulerInfo != null) {
      // LUCENE-5080: these two setters are removed, so we have to invoke setMaxMergesAndThreads
      // if someone has them configured.
      if (scheduler instanceof ConcurrentMergeScheduler) {
        NamedList args = mergeSchedulerInfo.initArgs.clone();
        Integer maxMergeCount = (Integer) args.remove("maxMergeCount");
        if (maxMergeCount == null) {
          maxMergeCount = ((ConcurrentMergeScheduler) scheduler).getMaxMergeCount();
        }
        Integer maxThreadCount = (Integer) args.remove("maxThreadCount");
        if (maxThreadCount == null) {
          maxThreadCount = ((ConcurrentMergeScheduler) scheduler).getMaxThreadCount();
        }
        ((ConcurrentMergeScheduler)scheduler).setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
        SolrPluginUtils.invokeSetters(scheduler, args);
      } else {
        SolrPluginUtils.invokeSetters(scheduler, mergeSchedulerInfo.initArgs);
      }
    }

    return scheduler;
  }

  public boolean getUseCompoundFile() {
    return effectiveUseCompountFileSetting;
  }

  /**
   * Lucene 4.4 removed the setUseCompoundFile(boolean) method from the two 
   * conrete MergePolicies provided with Lucene/Solr and added it to the 
   * IndexWriterConfig.  
   * In the event that users have a value explicitly configured for this 
   * setter in their MergePolicy init args, we remove it from the MergePolicy 
   * init args, update the 'effective' useCompoundFile setting used by the 
   * IndexWriterConfig, and warn about discontinuing to use this init arg.
   * 
   * @see #getUseCompoundFile
   */
  private void fixUseCFMergePolicyInitArg(Class c) {

    if (null == mergePolicyInfo || null == mergePolicyInfo.initArgs) return;

    Object useCFSArg = mergePolicyInfo.initArgs.remove("useCompoundFile");
    if (null != useCFSArg) {
      log.warn("Ignoring 'useCompoundFile' specified as an init arg for the <mergePolicy> since it is no directly longer supported by " + c.getSimpleName());
      if (useCFSArg instanceof Boolean) {
        boolean cfs = ((Boolean)useCFSArg).booleanValue();
        log.warn("Please update your config to specify <useCompoundFile>"+cfs+"</useCompoundFile> directly in your <indexConfig> settings.");
        effectiveUseCompountFileSetting = cfs;
      } else {
        log.error("MergePolicy's 'useCompoundFile' init arg is not a boolean, can not apply back compat logic to apply to the IndexWriterConfig: " + useCFSArg.toString());
      }
    }
  }
}
