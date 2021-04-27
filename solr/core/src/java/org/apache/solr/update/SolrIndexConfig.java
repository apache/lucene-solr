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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.InfoStream;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.index.DefaultMergePolicyFactory;
import org.apache.solr.index.MergePolicyFactory;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.apache.solr.index.SortingMergePolicy;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.XmlConfigFile.assertWarnOrFail;

/**
 * This config object encapsulates IndexWriter config params,
 * defined in the &lt;indexConfig&gt; section of solrconfig.xml
 */
public class SolrIndexConfig implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String NO_SUB_PACKAGES[] = new String[0];

  private static final String DEFAULT_MERGE_POLICY_FACTORY_CLASSNAME = DefaultMergePolicyFactory.class.getName();
  public static final String DEFAULT_MERGE_SCHEDULER_CLASSNAME = ConcurrentMergeScheduler.class.getName();

  public final boolean useCompoundFile;

  public final int maxBufferedDocs;

  public final double ramBufferSizeMB;
  public final int ramPerThreadHardLimitMB;
  /**
   * <p>
   * When using a custom merge policy that allows triggering synchronous merges on commit
   * (see {@link MergePolicy#findFullFlushMerges(org.apache.lucene.index.MergeTrigger, org.apache.lucene.index.SegmentInfos, org.apache.lucene.index.MergePolicy.MergeContext)}),
   * a timeout (in milliseconds) can be set for those merges to finish. Use {@code <maxCommitMergeWaitTime>1000</maxCommitMergeWaitTime>} in the {@code <indexConfig>} section.
   * See {@link IndexWriterConfig#setMaxFullFlushMergeWaitMillis(long)}.
   * </p>
   * <p>
   * Note that as of Solr 8.6, no {@code MergePolicy} shipped with Lucene/Solr make use of
   * {@code MergePolicy.findFullFlushMerges}, which means this setting has no effect unless a custom {@code MergePolicy} is used.
   * </p> 
   */
  public final int maxCommitMergeWaitMillis;

  public final int writeLockTimeout;
  public final String lockType;
  public final PluginInfo mergePolicyFactoryInfo;
  public final PluginInfo mergeSchedulerInfo;
  public final PluginInfo metricsInfo;
  
  public final PluginInfo mergedSegmentWarmerInfo;
  
  public InfoStream infoStream = InfoStream.NO_OUTPUT;
  private ConfigNode node;

  /**
   * Internal constructor for setting defaults based on Lucene Version
   */
  private SolrIndexConfig() {
    useCompoundFile = false;
    maxBufferedDocs = -1;
    ramBufferSizeMB = 100;
    ramPerThreadHardLimitMB = -1;
    maxCommitMergeWaitMillis = -1;
    writeLockTimeout = -1;
    lockType = DirectoryFactory.LOCK_TYPE_NATIVE;
    mergePolicyFactoryInfo = null;
    mergeSchedulerInfo = null;
    mergedSegmentWarmerInfo = null;
    // enable coarse-grained metrics by default
    metricsInfo = new PluginInfo("metrics", Collections.emptyMap(), null, null);
  }
  private ConfigNode get(String s) { return node.get(s); }
  public SolrIndexConfig(SolrConfig cfg, SolrIndexConfig def)  {
    this(cfg.get("indexConfig"), def);
  }
  /**
   * Constructs a SolrIndexConfig which parses the Lucene related config params in solrconfig.xml
   * @param def a SolrIndexConfig instance to pick default values from (optional)
   */
  public SolrIndexConfig(ConfigNode cfg, SolrIndexConfig def)  {
    this.node = cfg;
    if (def == null) {
      def = new SolrIndexConfig();
    }


    // sanity check: this will throw an error for us if there is more then one
    // config section
//    Object unused =  solrConfig.getNode(prefix, false);

    // Assert that end-of-life parameters or syntax is not in our config.
    // Warn for luceneMatchVersion's before LUCENE_3_6, fail fast above
    assertWarnOrFail("The <mergeScheduler>myclass</mergeScheduler> syntax is no longer supported in solrconfig.xml. Please use syntax <mergeScheduler class=\"myclass\"/> instead.",
        get("mergeScheduler").isNull() || get("mergeScheduler").attr("class") != null,
        true);
    assertWarnOrFail("Beginning with Solr 7.0, <mergePolicy>myclass</mergePolicy> is no longer supported, use <mergePolicyFactory> instead.",
        get("mergePolicy").isNull() || get("mergePolicy").attr("class") != null,
        true);
    assertWarnOrFail("The <luceneAutoCommit>true|false</luceneAutoCommit> parameter is no longer valid in solrconfig.xml.",
        get("luceneAutoCommit").isNull(),
        true);

    useCompoundFile = get("useCompoundFile").boolVal(def.useCompoundFile);
    maxBufferedDocs = get("maxBufferedDocs").intVal(def.maxBufferedDocs);
    ramBufferSizeMB = get("ramBufferSizeMB").doubleVal(def.ramBufferSizeMB);
    maxCommitMergeWaitMillis = get("maxCommitMergeWaitTime").intVal(def.maxCommitMergeWaitMillis);

    // how do we validate the value??
    ramPerThreadHardLimitMB = get("ramPerThreadHardLimitMB").intVal(def.ramPerThreadHardLimitMB);

    writeLockTimeout= get("writeLockTimeout").intVal(def.writeLockTimeout);
    lockType = get("lockType").txt(def.lockType);

    metricsInfo = getPluginInfo(get("metrics"), def.metricsInfo);
    mergeSchedulerInfo = getPluginInfo(get("mergeScheduler"), def.mergeSchedulerInfo);
    mergePolicyFactoryInfo = getPluginInfo(get("mergePolicyFactory"), def.mergePolicyFactoryInfo);

    assertWarnOrFail("Beginning with Solr 7.0, <mergePolicy> is no longer supported, use <mergePolicyFactory> instead.",
        get("mergePolicy").isNull(),
        true);
    assertWarnOrFail("Beginning with Solr 7.0, <maxMergeDocs> is no longer supported, configure it on the relevant <mergePolicyFactory> instead.",
        get("maxMergeDocs").isNull(),
        true);
    assertWarnOrFail("Beginning with Solr 7.0, <mergeFactor> is no longer supported, configure it on the relevant <mergePolicyFactory> instead.",
        get("maxMergeFactor").isNull(),
        true);

    if (get("termIndexInterval").exists()) {
      throw new IllegalArgumentException("Illegal parameter 'termIndexInterval'");
    }

    if(get("infoStream").boolVal(false)) {
      if (get("infoStream").attr("file") == null) {
        log.info("IndexWriter infoStream solr logging is enabled");
        infoStream = new LoggingInfoStream();
      } else {
        throw new IllegalArgumentException("Remove @file from <infoStream> to output messages to solr's logfile");
      }
    }
    mergedSegmentWarmerInfo = getPluginInfo(get("mergedSegmentWarmer"), def.mergedSegmentWarmerInfo);

    assertWarnOrFail("Beginning with Solr 5.0, <checkIntegrityAtMerge> option is no longer supported and should be removed from solrconfig.xml (these integrity checks are now automatic)",
        get( "checkIntegrityAtMerge").isNull(),
        true);
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    Map<String, Object> m = Utils.makeMap("useCompoundFile", useCompoundFile,
        "maxBufferedDocs", maxBufferedDocs,
        "ramBufferSizeMB", ramBufferSizeMB,
        "ramPerThreadHardLimitMB", ramPerThreadHardLimitMB,
        "maxCommitMergeWaitTime", maxCommitMergeWaitMillis,
        "writeLockTimeout", writeLockTimeout,
        "lockType", lockType,
        "infoStreamEnabled", infoStream != InfoStream.NO_OUTPUT);
    if(mergeSchedulerInfo != null) m.put("mergeScheduler",mergeSchedulerInfo);
    if (metricsInfo != null) {
      m.put("metrics", metricsInfo);
    }
    if (mergePolicyFactoryInfo != null) {
      m.put("mergePolicyFactory", mergePolicyFactoryInfo);
    }
    if(mergedSegmentWarmerInfo != null) m.put("mergedSegmentWarmer",mergedSegmentWarmerInfo);
    return m;
  }

  private PluginInfo getPluginInfo(ConfigNode node , PluginInfo def)  {
    return node != null && node.exists() ?
        new PluginInfo(node, "[solrconfig.xml] " + node.name(), false, false) :
        def;
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

  public IndexWriterConfig toIndexWriterConfig(SolrCore core) throws IOException {
    IndexSchema schema = core.getLatestSchema();
    IndexWriterConfig iwc = new IndexWriterConfig(new DelayedSchemaAnalyzer(core));
    if (maxBufferedDocs != -1)
      iwc.setMaxBufferedDocs(maxBufferedDocs);

    if (ramBufferSizeMB != -1)
      iwc.setRAMBufferSizeMB(ramBufferSizeMB);

    if (ramPerThreadHardLimitMB != -1) {
      iwc.setRAMPerThreadHardLimitMB(ramPerThreadHardLimitMB);
    }
    
    if (maxCommitMergeWaitMillis > 0) {
      iwc.setMaxFullFlushMergeWaitMillis(maxCommitMergeWaitMillis);
    }

    iwc.setSimilarity(schema.getSimilarity());
    MergePolicy mergePolicy = buildMergePolicy(core.getResourceLoader(), schema);
    iwc.setMergePolicy(mergePolicy);
    MergeScheduler mergeScheduler = buildMergeScheduler(core.getResourceLoader());
    iwc.setMergeScheduler(mergeScheduler);
    iwc.setInfoStream(infoStream);

    if (mergePolicy instanceof SortingMergePolicy) {
      Sort indexSort = ((SortingMergePolicy) mergePolicy).getSort();
      iwc.setIndexSort(indexSort);
    }

    iwc.setUseCompoundFile(useCompoundFile);

    if (mergedSegmentWarmerInfo != null) {
      // TODO: add infostream -> normal logging system (there is an issue somewhere)
      @SuppressWarnings({"rawtypes"})
      IndexReaderWarmer warmer = core.getResourceLoader().newInstance(mergedSegmentWarmerInfo.className,
                                                                        IndexReaderWarmer.class,
                                                                        null,
                                                                        new Class[] { InfoStream.class },
                                                                        new Object[] { iwc.getInfoStream() });
      iwc.setMergedSegmentWarmer(warmer);
    }

    return iwc;
  }

  /**
   * Builds a MergePolicy using the configured MergePolicyFactory
   * or if no factory is configured uses the configured mergePolicy PluginInfo.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private MergePolicy buildMergePolicy(SolrResourceLoader resourceLoader, IndexSchema schema) {

    final String mpfClassName;
    final MergePolicyFactoryArgs mpfArgs;
    if (mergePolicyFactoryInfo == null) {
      mpfClassName = DEFAULT_MERGE_POLICY_FACTORY_CLASSNAME;
      mpfArgs = new MergePolicyFactoryArgs();
    } else {
      mpfClassName = mergePolicyFactoryInfo.className;
      mpfArgs = new MergePolicyFactoryArgs(mergePolicyFactoryInfo.initArgs);
    }

    final MergePolicyFactory mpf = resourceLoader.newInstance(
        mpfClassName,
        MergePolicyFactory.class,
        NO_SUB_PACKAGES,
        new Class[] { SolrResourceLoader.class, MergePolicyFactoryArgs.class, IndexSchema.class },
        new Object[] {resourceLoader, mpfArgs, schema });

    return mpf.getMergePolicy();
  }

  @SuppressWarnings({"unchecked"})
  private MergeScheduler buildMergeScheduler(SolrResourceLoader resourceLoader) {
    String msClassName = mergeSchedulerInfo == null ? SolrIndexConfig.DEFAULT_MERGE_SCHEDULER_CLASSNAME : mergeSchedulerInfo.className;
    MergeScheduler scheduler = resourceLoader.newInstance(msClassName, MergeScheduler.class);

    if (mergeSchedulerInfo != null) {
      // LUCENE-5080: these two setters are removed, so we have to invoke setMaxMergesAndThreads
      // if someone has them configured.
      if (scheduler instanceof ConcurrentMergeScheduler) {
        @SuppressWarnings({"rawtypes"})
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
        Boolean ioThrottle = (Boolean) args.remove("ioThrottle");
        if (ioThrottle != null && !ioThrottle) { //by-default 'enabled'
            ((ConcurrentMergeScheduler) scheduler).disableAutoIOThrottle();
        }
        SolrPluginUtils.invokeSetters(scheduler, args);
      } else {
        SolrPluginUtils.invokeSetters(scheduler, mergeSchedulerInfo.initArgs);
      }
    }

    return scheduler;
  }

}
