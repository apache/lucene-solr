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

//
// For performance reasons, we don't want to re-read
// config params each time an index writer is created.
// This config object encapsulates IndexWriter config params.
//
/**
 * @version $Id$
 */
public class SolrIndexConfig {
  public static final String defaultsName ="indexDefaults";
  static final SolrIndexConfig defaultDefaults = new SolrIndexConfig();

  private SolrIndexConfig() {
    useCompoundFile = true;
    maxBufferedDocs = -1;
    maxMergeDocs = -1;
    mergeFactor = -1;
    maxFieldLength = -1;
    writeLockTimeout = -1;
    commitLockTimeout = -1;
    lockType = null;
  }
  
  public final boolean useCompoundFile;
  public final int maxBufferedDocs;
  public final int maxMergeDocs;
  public final int mergeFactor;
  public final int maxFieldLength;
  public final int writeLockTimeout;
  public final int commitLockTimeout;
  public final String lockType;

  public SolrIndexConfig(SolrConfig solrConfig, String prefix, SolrIndexConfig def)  {
    if (prefix == null)
      prefix = defaultsName;
    if (def == null)
      def = defaultDefaults;
    useCompoundFile=solrConfig.getBool(prefix+"/useCompoundFile", def.useCompoundFile);
    maxBufferedDocs=solrConfig.getInt(prefix+"/maxBufferedDocs",def.maxBufferedDocs);
    maxMergeDocs=solrConfig.getInt(prefix+"/maxMergeDocs",def.maxMergeDocs);
    mergeFactor=solrConfig.getInt(prefix+"/mergeFactor",def.mergeFactor);
    maxFieldLength=solrConfig.getInt(prefix+"/maxFieldLength",def.maxFieldLength);
    writeLockTimeout=solrConfig.getInt(prefix+"/writeLockTimeout", def.writeLockTimeout);
    commitLockTimeout=solrConfig.getInt(prefix+"/commitLockTimeout", def.commitLockTimeout);
    lockType=solrConfig.get(prefix+"/lockType", def.lockType);
  }
}
