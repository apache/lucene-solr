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
import java.util.List;
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

  public final int writeLockTimeout;
  public final String lockType;
  public final PluginInfo mergePolicyFactoryInfo;
  public final PluginInfo mergeSchedulerInfo;
  public final PluginInfo metricsInfo;
  
  public final PluginInfo mergedSegmentWarmerInfo;
  
  public InfoStream infoStream = InfoStream.NO_OUTPUT;

  /**
   * Internal constructor for setting defaults based on Lucene Version
   */
  private SolrIndexConfig(SolrConfig solrConfig) {
    useCompoundFile = false;
    maxBufferedDocs = -1;
    ramBufferSizeMB = 100;
    ramPerThreadHardLimitMB = -1;
    writeLockTimeout = -1;
    lockType = DirectoryFactory.LOCK_TYPE_NATIVE;
    mergePolicyFactoryInfo = null;
    mergeSchedulerInfo = null;
    mergedSegmentWarmerInfo = null;
    // enable coarse-grained metrics by default
    metricsInfo = new PluginInfo("metrics", Collections.emptyMap(), null, null);
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

    // Assert that end-of-life parameters or syntax is not in our config.
    // Warn for luceneMatchVersion's before LUCENE_3_6, fail fast above
    assertWarnOrFail("The <mergeScheduler>myclass</mergeScheduler> syntax is no longer supported in solrconfig.xml. Please use syntax <mergeScheduler class=\"myclass\"/> instead.",
        !((solrConfig.getNode(prefix + "/mergeScheduler", false) != null) && (solrConfig.get(prefix + "/mergeScheduler/@class", null) == null)),
        true);
    assertWarnOrFail("Beginning with Solr 7.0, <mergePolicy>myclass</mergePolicy> is no longer supported, use <mergePolicyFactory> instead.",
        !((solrConfig.getNode(prefix + "/mergePolicy", false) != null) && (solrConfig.get(prefix + "/mergePolicy/@class", null) == null)),
        true);
    assertWarnOrFail("The <luceneAutoCommit>true|false</luceneAutoCommit> parameter is no longer valid in solrconfig.xml.",
        solrConfig.get(prefix + "/luceneAutoCommit", null) == null,
        true);

    useCompoundFile = solrConfig.getBool(prefix+"/useCompoundFile", def.useCompoundFile);
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    ramBufferSizeMB = solrConfig.getDouble(prefix+"/ramBufferSizeMB", def.ramBufferSizeMB);

    // how do we validate the value??
    ramPerThreadHardLimitMB = solrConfig.getInt(prefix+"/ramPerThreadHardLimitMB", def.ramPerThreadHardLimitMB);

    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);

    List<PluginInfo> infos = solrConfig.readPluginInfos(prefix + "/metrics", false, false);
    if (infos.isEmpty()) {
      metricsInfo = def.metricsInfo;
    } else {
      metricsInfo = infos.get(0);
    }
    mergeSchedulerInfo = getPluginInfo(prefix + "/mergeScheduler", solrConfig, def.mergeSchedulerInfo);
    mergePolicyFactoryInfo = getPluginInfo(prefix + "/mergePolicyFactory", solrConfig, def.mergePolicyFactoryInfo);

    assertWarnOrFail("Beginning with Solr 7.0, <mergePolicy> is no longer supported, use <mergePolicyFactory> instead.",
        getPluginInfo(prefix + "/mergePolicy", solrConfig, null) == null,
        true);
    assertWarnOrFail("Beginning with Solr 7.0, <maxMergeDocs> is no longer supported, configure it on the relevant <mergePolicyFactory> instead.",
        solrConfig.getInt(prefix+"/maxMergeDocs", 0) == 0,
        true);
    assertWarnOrFail("Beginning with Solr 7.0, <mergeFactor> is no longer supported, configure it on the relevant <mergePolicyFactory> instead.",
        solrConfig.getInt(prefix+"/mergeFactor", 0) == 0,
        true);

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

    assertWarnOrFail("Beginning with Solr 5.0, <checkIntegrityAtMerge> option is no longer supported and should be removed from solrconfig.xml (these integrity checks are now automatic)",
        (null == solrConfig.getNode(prefix + "/checkIntegrityAtMerge", false)),
        true);
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    Map<String, Object> m = Utils.makeMap("useCompoundFile", useCompoundFile,
        "maxBufferedDocs", maxBufferedDocs,
        "ramBufferSizeMB", ramBufferSizeMB,
        "ramPerThreadHardLimitMB", ramPerThreadHardLimitMB,
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
  @SuppressWarnings("unchecked")
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

  private MergeScheduler buildMergeScheduler(SolrResourceLoader resourceLoader) {
    String msClassName = mergeSchedulerInfo == null ? SolrIndexConfig.DEFAULT_MERGE_SCHEDULER_CLASSNAME : mergeSchedulerInfo.className;
    MergeScheduler scheduler = resourceLoader.newInstance(msClassName, MergeScheduler.class);

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
