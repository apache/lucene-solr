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
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

//
// For performance reasons, we don't want to re-read
// config params each time an index writer is created.
// This config object encapsulates IndexWriter config params.
//
/**
 * @version $Id$
 */
public class SolrIndexConfig {
  public static final Logger log = LoggerFactory.getLogger(SolrIndexConfig.class);
  
  public static final String defaultsName ="indexDefaults";
  final String defaultMergePolicyClassName;
  public static final String DEFAULT_MERGE_SCHEDULER_CLASSNAME = ConcurrentMergeScheduler.class.getName();
  static final SolrIndexConfig defaultDefaults = new SolrIndexConfig();

  private SolrIndexConfig() {
    luceneVersion = Version.LUCENE_40;
    useCompoundFile = true;
    maxBufferedDocs = -1;
    maxMergeDocs = -1;
    mergeFactor = -1;
    ramBufferSizeMB = 16;
    writeLockTimeout = -1;
    commitLockTimeout = -1;
    lockType = null;
    termIndexInterval = IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL;
    mergePolicyInfo = null;
    mergeSchedulerInfo = null;
    defaultMergePolicyClassName = TieredMergePolicy.class.getName();
  }

  public final Version luceneVersion;
  
  public final boolean useCompoundFile;
  public final int maxBufferedDocs;
  public final int maxMergeDocs;
  public final int mergeFactor;

  public final double ramBufferSizeMB;

  public final int writeLockTimeout;
  public final int commitLockTimeout;
  public final String lockType;
  public final PluginInfo mergePolicyInfo;
  public final PluginInfo mergeSchedulerInfo;
  public final int termIndexInterval;
  
  public String infoStreamFile = null;

  public SolrIndexConfig(SolrConfig solrConfig, String prefix, SolrIndexConfig def)  {
    if (prefix == null)
      prefix = defaultsName;
    if (def == null)
      def = defaultDefaults;

    luceneVersion = solrConfig.luceneMatchVersion;

    defaultMergePolicyClassName = luceneVersion.onOrAfter(Version.LUCENE_33) ? TieredMergePolicy.class.getName() : LogByteSizeMergePolicy.class.getName();
    useCompoundFile=solrConfig.getBool(prefix+"/useCompoundFile", def.useCompoundFile);
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    maxMergeDocs=solrConfig.getInt(prefix+"/maxMergeDocs",def.maxMergeDocs);
    mergeFactor=solrConfig.getInt(prefix+"/mergeFactor",def.mergeFactor);
    ramBufferSizeMB = solrConfig.getDouble(prefix+"/ramBufferSizeMB", def.ramBufferSizeMB);

    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    commitLockTimeout=solrConfig.getInt(prefix+"/commitLockTimeout", def.commitLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);

    String str =  solrConfig.get(prefix+"/mergeScheduler/text()",null);
    if(str != null && str.trim().length() >0){
      //legacy handling <mergeScheduler>[classname]</mergeScheduler>
      //remove in Solr2.0
      log.warn("deprecated syntax : <mergeScheduler>[classname]</mergeScheduler>");
      Map<String,String> atrs = new HashMap<String, String>();
      atrs.put("class",str.trim());
      mergeSchedulerInfo = new PluginInfo("mergeScheduler",atrs,null,null);
    } else {
      mergeSchedulerInfo = getPluginInfo(prefix + "/mergeScheduler", solrConfig, def.mergeSchedulerInfo);
    }
    str =  solrConfig.get(prefix+"/mergePolicy/text()",null);
    if(str != null && str.trim().length() >0){
      //legacy handling  <mergePolicy>[classname]</mergePolicy>
      //remove in Solr2.0
      log.warn("deprecated syntax : <mergePolicy>[classname]</mergePolicy>");
      Map<String,String> atrs = new HashMap<String, String>();
      atrs.put("class",str.trim());
      mergePolicyInfo = new PluginInfo("mergePolicy",atrs,null,null);
    } else {
      mergePolicyInfo = getPluginInfo(prefix + "/mergePolicy", solrConfig, def.mergePolicyInfo);
    }
    
    Object luceneAutoCommit = solrConfig.get(prefix + "/luceneAutoCommit", null);
    if(luceneAutoCommit != null) {
      log.warn("found deprecated option : luceneAutoCommit no longer has any affect - it is always false");
    }
    
    termIndexInterval = solrConfig.getInt(prefix + "/termIndexInterval", def.termIndexInterval);
    
    boolean infoStreamEnabled = solrConfig.getBool(prefix + "/infoStream", false);
    if(infoStreamEnabled) {
      infoStreamFile= solrConfig.get(prefix + "/infoStream/@file", null);
      log.info("IndexWriter infoStream debug log is enabled: " + infoStreamFile);
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

    iwc.setSimilarityProvider(schema.getSimilarityProvider());
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
