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

import org.apache.solr.core.SolrConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final String DEFAULT_MERGE_POLICY_CLASSNAME = LogByteSizeMergePolicy.class.getName();
  public static final String DEFAULT_MERGE_SCHEDULER_CLASSNAME = ConcurrentMergeScheduler.class.getName();
  static final SolrIndexConfig defaultDefaults = new SolrIndexConfig();


  private SolrIndexConfig() {
    useCompoundFile = true;
    maxBufferedDocs = -1;
    maxMergeDocs = -1;
    mergeFactor = -1;
    ramBufferSizeMB = 16;
    maxFieldLength = -1;
    writeLockTimeout = -1;
    commitLockTimeout = -1;
    lockType = null;
    mergePolicyClassName = DEFAULT_MERGE_POLICY_CLASSNAME;
    mergeSchedulerClassname = DEFAULT_MERGE_SCHEDULER_CLASSNAME;
    luceneAutoCommit = false;
    termIndexInterval = IndexWriter.DEFAULT_TERM_INDEX_INTERVAL;
  }
  
  public final boolean useCompoundFile;
  public final int maxBufferedDocs;
  public final int maxMergeDocs;
  public final int mergeFactor;

  public final double ramBufferSizeMB;

  public final int maxFieldLength;
  public final int writeLockTimeout;
  public final int commitLockTimeout;
  public final String lockType;
  public final String mergePolicyClassName;
  public final String mergeSchedulerClassname;
  public final boolean luceneAutoCommit;
  public final int termIndexInterval;
  
  public String infoStreamFile = null;

  public SolrIndexConfig(SolrConfig solrConfig, String prefix, SolrIndexConfig def)  {
    if (prefix == null)
      prefix = defaultsName;
    if (def == null)
      def = defaultDefaults;
    useCompoundFile=solrConfig.getBool(prefix+"/useCompoundFile", def.useCompoundFile);
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    maxMergeDocs=solrConfig.getInt(prefix+"/maxMergeDocs",def.maxMergeDocs);
    mergeFactor=solrConfig.getInt(prefix+"/mergeFactor",def.mergeFactor);
    ramBufferSizeMB = solrConfig.getDouble(prefix+"/ramBufferSizeMB", def.ramBufferSizeMB);

    maxFieldLength=solrConfig.getInt(prefix+"/maxFieldLength",def.maxFieldLength);
    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    commitLockTimeout=solrConfig.getInt(prefix+"/commitLockTimeout", def.commitLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);
    mergePolicyClassName = solrConfig.get(prefix + "/mergePolicy", def.mergePolicyClassName);
    mergeSchedulerClassname = solrConfig.get(prefix + "/mergeScheduler", def.mergeSchedulerClassname);
    luceneAutoCommit = solrConfig.getBool(prefix + "/luceneAutoCommit", def.luceneAutoCommit);
    termIndexInterval = solrConfig.getInt(prefix + "/termIndexInterval", def.termIndexInterval);
    
    boolean infoStreamEnabled = solrConfig.getBool(prefix + "/infoStream", false);
    if(infoStreamEnabled) {
      infoStreamFile= solrConfig.get(prefix + "/infoStream/@file", null);
      log.info("IndexWriter infoStream debug log is enabled: " + infoStreamFile);
    }

  }
}
