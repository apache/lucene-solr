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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.internal */
public class IndexFingerprint implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private long maxVersionSpecified;
  private long maxVersionEncountered;
  // this actually means max versions used in computing the hash.
  // we cannot change this now because it changes back-compat
  private long maxInHash;
  private long versionsHash;
  private long numVersions;
  private long numDocs;
  private long maxDoc;

  public IndexFingerprint() {
    // default constructor
  }
  
  public IndexFingerprint (long maxVersionSpecified)  {
    this.maxVersionSpecified = maxVersionSpecified;
  }
  
  public long getMaxVersionSpecified() {
    return maxVersionSpecified;
  }

  public long getMaxVersionEncountered() {
    return maxVersionEncountered;
  }

  public long getMaxInHash() {
    return maxInHash;
  }

  public long getVersionsHash() {
    return versionsHash;
  }

  public long getNumVersions() {
    return numVersions;
  }

  public long getNumDocs() {
    return numDocs;
  }

  public long getMaxDoc() {
    return maxDoc;
  }

  /** Opens a new realtime searcher and returns it's (possibly cached) fingerprint */
  public static IndexFingerprint getFingerprint(SolrCore core, long maxVersion) throws IOException {
    RTimer timer = new RTimer();
    core.getUpdateHandler().getUpdateLog().openRealtimeSearcher();
    RefCounted<SolrIndexSearcher> newestSearcher = core.getUpdateHandler().getUpdateLog().uhandler.core.getRealtimeSearcher();
    try {
      IndexFingerprint f = newestSearcher.get().getIndexFingerprint(maxVersion);
      final double duration = timer.stop();
      log.info("IndexFingerprint millis:{} result:{}",duration, f);
      return f;
    } finally {
      if (newestSearcher != null) {
        newestSearcher.decref();
      }
    }
  }
  
  @SuppressWarnings({"unchecked"})
  public static IndexFingerprint getFingerprint(SolrIndexSearcher searcher, LeafReaderContext ctx, Long maxVersion)
      throws IOException {
    SchemaField versionField = VersionInfo.getAndCheckVersionField(searcher.getSchema());
    ValueSource vs = versionField.getType().getValueSource(versionField, null);
    @SuppressWarnings({"rawtypes"})
    Map funcContext = ValueSource.newContext(searcher);
    vs.createWeight(funcContext, searcher);
    
    IndexFingerprint f = new IndexFingerprint();
    f.maxVersionSpecified = maxVersion;
    f.maxDoc = ctx.reader().maxDoc();
    f.numDocs = ctx.reader().numDocs();
    
    int maxDoc = ctx.reader().maxDoc();
    Bits liveDocs = ctx.reader().getLiveDocs();
    FunctionValues fv = vs.getValues(funcContext, ctx);
    for (int doc = 0; doc < maxDoc; doc++) {
      if (liveDocs != null && !liveDocs.get(doc)) continue;
      long v = fv.longVal(doc);
      f.maxVersionEncountered = Math.max(v, f.maxVersionEncountered);
      if (v <= f.maxVersionSpecified) {
        f.maxInHash = Math.max(v, f.maxInHash);
        f.versionsHash += Hash.fmix64(v);
        f.numVersions++;
      }
    }
    
    return f;
  }
  
  
  public static IndexFingerprint reduce(IndexFingerprint acc, IndexFingerprint f2) {
    // acc should have maxVersionSpecified already set in it using IndexFingerprint(long maxVersionSpecified) constructor
    acc.maxDoc = Math.max(acc.maxDoc, f2.maxDoc);
    acc.numDocs += f2.numDocs;
    acc.maxVersionEncountered = Math.max(acc.maxVersionEncountered, f2.maxVersionEncountered);
    acc.maxInHash = Math.max(acc.maxInHash, f2.maxInHash);
    acc.versionsHash += f2.versionsHash;
    acc.numVersions += f2.numVersions;

    return acc;
  }

  /** returns 0 for equal, negative if f1 is less recent than f2, positive if more recent */
  public static int compare(IndexFingerprint f1, IndexFingerprint f2) {
    int cmp;

    // NOTE: some way want number of docs in index to take precedence over highest version (add-only systems for sure)

    // if we're comparing all of the versions in the index, then go by the highest encountered.
    if (f1.maxVersionSpecified == Long.MAX_VALUE) {
      cmp = Long.compare(f1.maxVersionEncountered, f2.maxVersionEncountered);
      if (cmp != 0) return cmp;
    }

    // Go by the highest version under the requested max.
    cmp = Long.compare(f1.maxInHash, f2.maxInHash);
    if (cmp != 0) return cmp;

    // go by who has the most documents in the index
    cmp = Long.compare(f1.numVersions, f2.numVersions);
    if (cmp != 0) return cmp;

    // both have same number of documents, so go by hash
    cmp = Long.compare(f1.versionsHash, f2.versionsHash);
    return cmp;
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    map.put("maxVersionSpecified", maxVersionSpecified);
    map.put("maxVersionEncountered", maxVersionEncountered);
    map.put("maxInHash", maxInHash);
    map.put("versionsHash", versionsHash);
    map.put("numVersions", numVersions);
    map.put("numDocs", numDocs);
    map.put("maxDoc", maxDoc);
    return map;
  }

  private static long getLong(@SuppressWarnings({"rawtypes"})Map m, String key, long def) {
    Object oval = m.get(key);
    return oval != null ? ((Number)oval).longValue() : def;
  }

  /**
   * Create an IndexFingerprint object from a deserialized generic object (Map or NamedList)
   */
  public static IndexFingerprint fromObject(Object o) {
    if (o instanceof IndexFingerprint) return (IndexFingerprint) o;
    @SuppressWarnings({"rawtypes"})
    Map map = null;
    if (o instanceof Map) {
      map = (Map) o;
    } else if (o instanceof NamedList) {
      map = ((NamedList) o).asShallowMap();
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type " + o);
    }
    IndexFingerprint f = new IndexFingerprint();
    f.maxVersionSpecified = getLong(map, "maxVersionSpecified", Long.MAX_VALUE);
    f.maxVersionEncountered = getLong(map, "maxVersionEncountered", -1);
    f.maxInHash = getLong(map, "maxInHash", -1);
    f.versionsHash = getLong(map, "versionsHash", -1);
    f.numVersions = getLong(map, "numVersions", -1);
    f.numDocs = getLong(map, "numDocs", -1);
    f.maxDoc = getLong(map, "maxDoc", -1);
    return f;
  }

  @Override
  public String toString() {
    return toMap(new LinkedHashMap<>()).toString();
  }

}
