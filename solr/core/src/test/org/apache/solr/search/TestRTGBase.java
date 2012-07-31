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
package org.apache.solr.search;


import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.update.UpdateLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class TestRTGBase extends SolrTestCaseJ4 {

  // means we've seen the leader and have version info (i.e. we are a non-leader replica)
  public static String FROM_LEADER = DistribPhase.FROMLEADER.toString();

  // since we make up fake versions in these tests, we can get messed up by a DBQ with a real version
  // since Solr can think following updates were reordered.
  @Override
  public void clearIndex() {
    try {
      deleteByQueryAndGetVersion("*:*", params("_version_", Long.toString(-Long.MAX_VALUE), DISTRIB_UPDATE_PARAM,FROM_LEADER));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected final ConcurrentHashMap<Integer,DocInfo> model = new ConcurrentHashMap<Integer,DocInfo>();
  protected Map<Integer,DocInfo> committedModel = new HashMap<Integer,DocInfo>();
  protected long snapshotCount;
  protected long committedModelClock;
  protected volatile int lastId;
  protected final String field = "val_l";
  protected Object[] syncArr;

  protected Object globalLock = this;

  protected void initModel(int ndocs) {
    snapshotCount = 0;
    committedModelClock = 0;
    lastId = 0;

    syncArr = new Object[ndocs];

    for (int i=0; i<ndocs; i++) {
      model.put(i, new DocInfo(0, -1L));
      syncArr[i] = new Object();
    }
    committedModel.putAll(model);
  }


  protected static class DocInfo {
    long version;
    long val;

    public DocInfo(long version, long val) {
      this.version = version;
      this.val = val;
    }

    public String toString() {
      return "{version="+version+",val="+val+"}";
    }
  }

  protected long badVersion(Random rand, long version) {
    if (version > 0) {
      // return a random number not equal to version
      for (;;) {
        long badVersion = rand.nextInt();
        if (badVersion != version && badVersion != 0) return badVersion;
      }
    }

    // if the version does not exist, then we can only specify a positive version
    for (;;) {
      long badVersion = rand.nextInt() & 0x7fffffff;  // mask off sign bit
      if (badVersion != 0) return badVersion;
    }
  }


  protected List<Long> getLatestVersions() {
    List<Long> recentVersions;
    UpdateLog.RecentUpdates startingRecentUpdates = h.getCore().getUpdateHandler().getUpdateLog().getRecentUpdates();
    try {
      recentVersions = startingRecentUpdates.getVersions(100);
    } finally {
      startingRecentUpdates.close();
    }
    return recentVersions;
  }



  protected int getFirstMatch(IndexReader r, Term t) throws IOException {
    Fields fields = MultiFields.getFields(r);
    if (fields == null) return -1;
    Terms terms = fields.terms(t.field());
    if (terms == null) return -1;
    BytesRef termBytes = t.bytes();
    final TermsEnum termsEnum = terms.iterator(null);
    if (!termsEnum.seekExact(termBytes, false)) {
      return -1;
    }
    DocsEnum docs = termsEnum.docs(MultiFields.getLiveDocs(r), null, false);
    int id = docs.nextDoc();
    if (id != DocIdSetIterator.NO_MORE_DOCS) {
      int next = docs.nextDoc();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
    }
    return id == DocIdSetIterator.NO_MORE_DOCS ? -1 : id;
  }

}
