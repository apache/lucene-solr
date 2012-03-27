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

package org.apache.solr.update;

import org.apache.lucene.index.*;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This config object encapsulates IndexWriter config params,
 * defined in the &lt;indexConfig&gt; section of solrconfig.xml
 * @version $Id$
 */
public class SolrIndexConfig {
  public static final Logger log = LoggerFactory.getLogger(SolrIndexConfig.class);
  
  final String defaultMergePolicyClassName;
  public static final String DEFAULT_MERGE_SCHEDULER_CLASSNAME = ConcurrentMergeScheduler.class.getName();
  public final Version luceneVersion;
  
  public final boolean useCompoundFile;
  public final int maxBufferedDocs;
  public final int maxMergeDocs;
  public final int mergeFactor;

  public final double ramBufferSizeMB;

  public final int maxFieldLength;
  public final int writeLockTimeout;
  public final String lockType;
  public final PluginInfo mergePolicyInfo;
  public final PluginInfo mergeSchedulerInfo;
  public final int termIndexInterval;
  
  public String infoStreamFile = null;

  /**
   * Internal constructor for setting defaults based on Lucene Version
   */
  private SolrIndexConfig(SolrConfig solrConfig) {
    luceneVersion = solrConfig.luceneMatchVersion;
    useCompoundFile = luceneVersion.onOrAfter(Version.LUCENE_36) ? false : true;
    maxBufferedDocs = -1;
    maxMergeDocs = -1;
    mergeFactor = -1;
    ramBufferSizeMB = luceneVersion.onOrAfter(Version.LUCENE_36) ? 32 : 16;
    maxFieldLength = -1;
    writeLockTimeout = -1;
    lockType = luceneVersion.onOrAfter(Version.LUCENE_36) ? 
               SolrIndexWriter.LOCK_TYPE_NATIVE : 
               SolrIndexWriter.LOCK_TYPE_SIMPLE;
    termIndexInterval = IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL;
    mergePolicyInfo = null;
    mergeSchedulerInfo = null;
    defaultMergePolicyClassName = luceneVersion.onOrAfter(Version.LUCENE_33) ? TieredMergePolicy.class.getName() : LogByteSizeMergePolicy.class.getName();
  }
  
  /**
   * Constructs a SolrIndexConfig which parses the Lucene related config params in solrconfig.xml
   * @param solrConfig the overall SolrConfig object
   * @param prefix the XPath prefix for which section to parse (mandatory)
   * @param def a SolrIndexConfig instance to pick default values from (optional)
   */
  public SolrIndexConfig(SolrConfig solrConfig, String prefix, SolrIndexConfig def)  {
    if (prefix == null)
      throw new SolrException(ErrorCode.FORBIDDEN, "Prefix was null");
    if (def == null)
      def = new SolrIndexConfig(solrConfig);

    luceneVersion = solrConfig.luceneMatchVersion;

    // Assert that end-of-life parameters or syntax is not in our config.
    // Warn for luceneMatchVersion's before LUCENE_36, fail fast above
    assertWarnOrFail("The <mergeScheduler>myclass</mergeScheduler> syntax is no longer supported in solrconfig.xml. Please use syntax <mergeScheduler class=\"myclass\"/> instead.",
        !((solrConfig.get(prefix+"/mergeScheduler/text()",null) != null) && (solrConfig.get(prefix+"/mergeScheduler/@class",null) == null)),
        luceneVersion.onOrAfter(Version.LUCENE_36));
    assertWarnOrFail("The <mergePolicy>myclass</mergePolicy> syntax is no longer supported in solrconfig.xml. Please use syntax <mergePolicy class=\"myclass\"/> instead.",
        !((solrConfig.get(prefix+"/mergePolicy/text()",null) != null) && (solrConfig.get(prefix+"/mergePolicy/@class",null) == null)),
        luceneVersion.onOrAfter(Version.LUCENE_36));
    assertWarnOrFail("The <luceneAutoCommit>true|false</luceneAutoCommit> parameter is no longer valid in solrconfig.xml.",
        solrConfig.get(prefix+"/luceneAutoCommit", null) == null,
        luceneVersion.onOrAfter(Version.LUCENE_36));

    defaultMergePolicyClassName = def.defaultMergePolicyClassName;
    useCompoundFile=solrConfig.getBool(prefix+"/useCompoundFile", def.useCompoundFile);
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    maxMergeDocs=solrConfig.getInt(prefix+"/maxMergeDocs",def.maxMergeDocs);
    mergeFactor=solrConfig.getInt(prefix+"/mergeFactor",def.mergeFactor);
    ramBufferSizeMB = solrConfig.getDouble(prefix+"/ramBufferSizeMB", def.ramBufferSizeMB);

    maxFieldLength=solrConfig.getInt(prefix+"/maxFieldLength",def.maxFieldLength);
    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);

    mergeSchedulerInfo = getPluginInfo(prefix + "/mergeScheduler", solrConfig, def.mergeSchedulerInfo);
    mergePolicyInfo = getPluginInfo(prefix + "/mergePolicy", solrConfig, def.mergePolicyInfo);
    
    termIndexInterval = solrConfig.getInt(prefix + "/termIndexInterval", def.termIndexInterval);
    
    boolean infoStreamEnabled = solrConfig.getBool(prefix + "/infoStream", false);
    if(infoStreamEnabled) {
      infoStreamFile= solrConfig.get(prefix + "/infoStream/@file", null);
      log.info("IndexWriter infoStream debug log is enabled: " + infoStreamFile);
    }
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
    IndexWriterConfig iwc = new IndexWriterConfig(luceneVersion, schema.getAnalyzer());
    if (maxBufferedDocs != -1)
      iwc.setMaxBufferedDocs(maxBufferedDocs);

    if (ramBufferSizeMB != -1)
      iwc.setRAMBufferSizeMB(ramBufferSizeMB);

    if (termIndexInterval != -1)
      iwc.setTermIndexInterval(termIndexInterval);

    if (writeLockTimeout != -1)
      iwc.setWriteLockTimeout(writeLockTimeout);

    iwc.setSimilarity(schema.getSimilarity());
    iwc.setMergePolicy(buildMergePolicy(schema));
    iwc.setMergeScheduler(buildMergeScheduler(schema));

    return iwc;
  }

  private MergePolicy buildMergePolicy(IndexSchema schema) {
    MergePolicy policy;
    String mpClassName = mergePolicyInfo == null ? defaultMergePolicyClassName : mergePolicyInfo.className;

    try {
      policy = (MergePolicy) schema.getResourceLoader().newInstance(mpClassName, null, new Class[]{IndexWriter.class}, new Object[]{this});
    } catch (Exception e) {
      policy = (MergePolicy) schema.getResourceLoader().newInstance(mpClassName);
    }

    if (policy instanceof LogMergePolicy) {
      LogMergePolicy logMergePolicy = (LogMergePolicy) policy;

      if (maxMergeDocs != -1)
        logMergePolicy.setMaxMergeDocs(maxMergeDocs);

      logMergePolicy.setUseCompoundFile(useCompoundFile);

      if (mergeFactor != -1)
        logMergePolicy.setMergeFactor(mergeFactor);
    } else if (policy instanceof TieredMergePolicy) {
      TieredMergePolicy tieredMergePolicy = (TieredMergePolicy) policy;
      
      tieredMergePolicy.setUseCompoundFile(useCompoundFile);
      
      if (mergeFactor != -1) {
        tieredMergePolicy.setMaxMergeAtOnce(mergeFactor);
        tieredMergePolicy.setSegmentsPerTier(mergeFactor);
      }
    } else {
      log.warn("Use of compound file format or mergefactor cannot be configured if merge policy is not an instance of LogMergePolicy or TieredMergePolicy. The configured policy's defaults will be used.");
    }

    if (mergePolicyInfo != null)
      SolrPluginUtils.invokeSetters(policy, mergePolicyInfo.initArgs);

    return policy;
  }

  private MergeScheduler buildMergeScheduler(IndexSchema schema) {
    String msClassName = mergeSchedulerInfo == null ? SolrIndexConfig.DEFAULT_MERGE_SCHEDULER_CLASSNAME : mergeSchedulerInfo.className;
    MergeScheduler scheduler = (MergeScheduler) schema.getResourceLoader().newInstance(msClassName);

    if (mergeSchedulerInfo != null)
      SolrPluginUtils.invokeSetters(scheduler, mergeSchedulerInfo.initArgs);

    return scheduler;
  }
}
