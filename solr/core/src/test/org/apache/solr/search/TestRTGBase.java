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


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.update.UpdateLog;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;

public class TestRTGBase extends SolrTestCaseJ4 {

  // means we've seen the leader and have version info (i.e. we are a non-leader replica)
  public static String FROM_LEADER = DistribPhase.FROMLEADER.toString();

  protected final ConcurrentHashMap<Integer,DocInfo> model = new ConcurrentHashMap<>();
  protected Map<Integer,DocInfo> committedModel = new HashMap<>();
  protected long snapshotCount;
  protected long committedModelClock;
  protected volatile int lastId;
  protected static final String FIELD = "val_l";
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

    @Override
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
    try (UpdateLog.RecentUpdates startingRecentUpdates = h.getCore().getUpdateHandler().getUpdateLog().getRecentUpdates()) {
      return startingRecentUpdates.getVersions(100);
    }
  }



  protected int getFirstMatch(IndexReader r, Term t) throws IOException {
    Terms terms = MultiTerms.getTerms(r, t.field());
    if (terms == null) return -1;
    BytesRef termBytes = t.bytes();
    final TermsEnum termsEnum = terms.iterator();
    if (!termsEnum.seekExact(termBytes)) {
      return -1;
    }
    PostingsEnum docs = termsEnum.postings(null, PostingsEnum.NONE);
    docs = BitsFilteredPostingsEnum.wrap(docs, MultiBits.getLiveDocs(r));
    int id = docs.nextDoc();
    if (id != DocIdSetIterator.NO_MORE_DOCS) {
      int next = docs.nextDoc();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
    }
    return id == DocIdSetIterator.NO_MORE_DOCS ? -1 : id;
  }

}
