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

import static org.apache.solr.core.Config.assertWarnOrFail;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.MapSerializable;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.index.DefaultMergePolicyFactory;
import org.apache.solr.index.MergePolicyFactory;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This config object encapsulates IndexWriter config params,
 * defined in the &lt;indexConfig&gt; section of solrconfig.xml
 */
public class SolrIndexConfig implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String NO_SUB_PACKAGES[] = new String[0];

  private static final String DEFAULT_MERGE_POLICY_FACTORY_CLASSNAME = DefaultMergePolicyFactory.class.getName();
  public static final String DEFAULT_MERGE_SCHEDULER_CLASSNAME = ConcurrentMergeScheduler.class.getName();
  public final Version luceneVersion;

  private boolean effectiveUseCompoundFileSetting;

  public final int maxBufferedDocs;
  public final int maxMergeDocs;
  public final int mergeFactor;

  public final double ramBufferSizeMB;

  public final int writeLockTimeout;
  public final String lockType;
  public final PluginInfo mergePolicyInfo;
  public final PluginInfo mergePolicyFactoryInfo;
  public final PluginInfo mergeSchedulerInfo;
  
  public final PluginInfo mergedSegmentWarmerInfo;
  
  public InfoStream infoStream = InfoStream.NO_OUTPUT;

  /**
   * Internal constructor for setting defaults based on Lucene Version
   */
  private SolrIndexConfig(SolrConfig solrConfig) {
    luceneVersion = solrConfig.luceneMatchVersion;
    effectiveUseCompoundFileSetting = false;
    maxBufferedDocs = -1;
    maxMergeDocs = -1;
    mergeFactor = -1;
    ramBufferSizeMB = 100;
    writeLockTimeout = -1;
    lockType = DirectoryFactory.LOCK_TYPE_NATIVE;
    mergePolicyInfo = null;
    mergePolicyFactoryInfo = null;
    mergeSchedulerInfo = null;
    mergedSegmentWarmerInfo = null;
  }
  
  /**
   * Constructs a SolrIndexConfig which parses the Lucene related config params in solrconfig.xml
   * @param solrConfig the overall SolrConfig object
   * @param prefix the XPath prefix for which section to parse (mandatory)
   * @param def a SolrIndexConfig instance to pick default values from (optional)
   */
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
        !((solrConfig.getNode(prefix + "/mergeScheduler", false) != null) && (solrConfig.get(prefix + "/mergeScheduler/@class", null) == null)),
        true);
    assertWarnOrFail("The <mergePolicy>myclass</mergePolicy> syntax is no longer supported in solrconfig.xml. Please use syntax <mergePolicy class=\"myclass\"/> instead.",
        !((solrConfig.getNode(prefix + "/mergePolicy", false) != null) && (solrConfig.get(prefix + "/mergePolicy/@class", null) == null)),
        true);
    assertWarnOrFail("The <luceneAutoCommit>true|false</luceneAutoCommit> parameter is no longer valid in solrconfig.xml.",
        solrConfig.get(prefix + "/luceneAutoCommit", null) == null,
        true);

    effectiveUseCompoundFileSetting = solrConfig.getBool(prefix+"/useCompoundFile", def.getUseCompoundFile());
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    maxMergeDocs=solrConfig.getInt(prefix+"/maxMergeDocs",def.maxMergeDocs);
    mergeFactor=solrConfig.getInt(prefix+"/mergeFactor",def.mergeFactor);
    ramBufferSizeMB = solrConfig.getDouble(prefix+"/ramBufferSizeMB", def.ramBufferSizeMB);

    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);

    mergeSchedulerInfo = getPluginInfo(prefix + "/mergeScheduler", solrConfig, def.mergeSchedulerInfo);
    mergePolicyInfo = getPluginInfo(prefix + "/mergePolicy", solrConfig, def.mergePolicyInfo);
    mergePolicyFactoryInfo = getPluginInfo(prefix + "/mergePolicyFactory", solrConfig, def.mergePolicyInfo);
    if (mergePolicyInfo != null && mergePolicyFactoryInfo != null) {
      throw new IllegalArgumentException("<mergePolicy> and <mergePolicyFactory> are mutually exclusive.");
    }

    assertWarnOrFail("Beginning with Solr 5.5, <mergePolicy> is deprecated, use <mergePolicyFactory> instead.",
        (mergePolicyInfo == null), false);
    assertWarnOrFail("Beginning with Solr 5.5, <maxMergeDocs> is deprecated, configure it on the relevant <mergePolicyFactory> instead.",
        (mergePolicyFactoryInfo != null && maxMergeDocs == def.maxMergeDocs), false);
    assertWarnOrFail("Beginning with Solr 5.5, <mergeFactor> is deprecated, configure it on the relevant <mergePolicyFactory> instead.",
        (mergePolicyFactoryInfo != null && mergeFactor == def.mergeFactor), false);

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

    assertWarnOrFail("Begining with Solr 5.0, <checkIntegrityAtMerge> option is no longer supported and should be removed from solrconfig.xml (these integrity checks are now automatic)",
        (null == solrConfig.getNode(prefix + "/checkIntegrityAtMerge", false)),
        false);
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> m = Utils.makeMap("useCompoundFile", effectiveUseCompoundFileSetting,
        "maxBufferedDocs", maxBufferedDocs,
        "maxMergeDocs", maxMergeDocs,
        "mergeFactor", mergeFactor,
        "ramBufferSizeMB", ramBufferSizeMB,
        "writeLockTimeout", writeLockTimeout,
        "lockType", lockType,
        "infoStreamEnabled", infoStream != InfoStream.NO_OUTPUT);
    if(mergeSchedulerInfo != null) m.put("mergeScheduler",mergeSchedulerInfo.toMap());
    if (mergePolicyInfo != null) {
      m.put("mergePolicy", mergePolicyInfo.toMap());
    } else if (mergePolicyFactoryInfo != null) {
      m.put("mergePolicy", mergePolicyFactoryInfo.toMap());
    }
    if(mergedSegmentWarmerInfo != null) m.put("mergedSegmentWarmer",mergedSegmentWarmerInfo.toMap());
    return m;
  }

  private PluginInfo getPluginInfo(String path, SolrConfig solrConfig, PluginInfo def)  {
    List<PluginInfo> l = solrConfig.readPluginInfos(path, false, true);
    return l.isEmpty() ? def : l.get(0);
  }

  private static class DelayedSchemaAnalyzer extends DelegatingAnalyzerWrapper {
    private final SolrCore core;

    public DelayedSchemaAnalyzer(SolrCore core) {
      super(PER_FIELD_REUSE_STRATEGY);
      this.core = core;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
      return core.getLatestSchema().getIndexAnalyzer();
    }
  }

  public IndexWriterConfig toIndexWriterConfig(SolrCore core) {
    IndexSchema schema = core.getLatestSchema();
    IndexWriterConfig iwc = new IndexWriterConfig(new DelayedSchemaAnalyzer(core));
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

  private boolean useMergePolicyInfo() {
    return mergePolicyInfo != null || maxMergeDocs != -1 || mergeFactor != -1;
  }

  /**
   * Builds a MergePolicy using the configured MergePolicyFactory
   * or if no factory is configured uses the configured mergePolicy PluginInfo.
   */
  @SuppressWarnings("unchecked")
  private MergePolicy buildMergePolicy(final IndexSchema schema) {
    if (useMergePolicyInfo()) {
      return buildMergePolicyFromInfo(schema);
    }

    final String mpfClassName;
    final MergePolicyFactoryArgs mpfArgs;
    if (mergePolicyFactoryInfo == null) {
      mpfClassName = DEFAULT_MERGE_POLICY_FACTORY_CLASSNAME;
      mpfArgs = new MergePolicyFactoryArgs();
    } else {
      mpfClassName = mergePolicyFactoryInfo.className;
      mpfArgs = new MergePolicyFactoryArgs(mergePolicyFactoryInfo.initArgs);
    }

    final SolrResourceLoader resourceLoader = schema.getResourceLoader();
    final MergePolicyFactory mpf = resourceLoader.newInstance(
        mpfClassName,
        MergePolicyFactory.class,
        NO_SUB_PACKAGES,
        new Class[] { SolrResourceLoader.class, MergePolicyFactoryArgs.class, IndexSchema.class },
        new Object[] { resourceLoader, mpfArgs, schema });

    return mpf.getMergePolicy();
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
  private MergePolicy buildMergePolicyFromInfo(IndexSchema schema) {
    final MergePolicy policy;
    if (mergePolicyInfo == null) {
      final SolrResourceLoader resourceLoader = schema.getResourceLoader();
      final MergePolicyFactoryArgs mpfArgs = new MergePolicyFactoryArgs();
      final MergePolicyFactory defaultMergePolicyFactory = resourceLoader.newInstance(
          DEFAULT_MERGE_POLICY_FACTORY_CLASSNAME,
          MergePolicyFactory.class,
          NO_SUB_PACKAGES,
          new Class[] { SolrResourceLoader.class, MergePolicyFactoryArgs.class, IndexSchema.class },
          new Object[] { resourceLoader, mpfArgs, schema });
      policy = defaultMergePolicyFactory.getMergePolicy();
    } else {
      policy = schema.getResourceLoader().newInstance(mergePolicyInfo.className, MergePolicy.class);
    }

    if (policy instanceof LogMergePolicy) {
      LogMergePolicy logMergePolicy = (LogMergePolicy) policy;
      fixUseCFMergePolicyInitArg(LogMergePolicy.class);

      if (maxMergeDocs != -1)
        logMergePolicy.setMaxMergeDocs(maxMergeDocs);

      if (mergeFactor != -1)
        logMergePolicy.setMergeFactor(mergeFactor);
    } else if (policy instanceof TieredMergePolicy) {
      TieredMergePolicy tieredMergePolicy = (TieredMergePolicy) policy;
      fixUseCFMergePolicyInitArg(TieredMergePolicy.class);

      if (mergeFactor != -1) {
        tieredMergePolicy.setMaxMergeAtOnce(mergeFactor);
        tieredMergePolicy.setSegmentsPerTier(mergeFactor);
      }
    } else if (mergeFactor != -1) {
      log.warn("Use of <mergeFactor> cannot be configured if merge policy is not an instance of LogMergePolicy or TieredMergePolicy. The configured policy's defaults will be used.");
    }

    if (mergePolicyInfo != null) {
      SolrPluginUtils.invokeSetters(policy, mergePolicyInfo.initArgs);
    }

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
    return effectiveUseCompoundFileSetting;
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
        effectiveUseCompoundFileSetting = cfs;
      } else {
        log.error("MergePolicy's 'useCompoundFile' init arg is not a boolean, can not apply back compat logic to apply to the IndexWriterConfig: " + useCFSArg.toString());
      }
    }
  }
}
