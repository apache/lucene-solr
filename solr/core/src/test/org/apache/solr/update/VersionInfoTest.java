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

import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Hash;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.Test;

public class VersionInfoTest extends SolrTestCaseJ4 {

  @Test
  public void testMaxIndexedVersionFromIndex() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-version-indexed.xml");
    try (SolrQueryRequest r = req()) {
      SchemaField v = r.getCore().getUpdateHandler().getUpdateLog().getVersionInfo().getVersionField();
      assertNotNull(v);
      assertTrue(v.indexed());
      assertFalse(v.hasDocValues());
      
      testMaxVersionLogic(r);
    } finally {
      deleteCore();
    }
  }

  @Test
  public void testMaxDocValuesVersionFromIndex() throws Exception {
    initCore("solrconfig-tlog.xml","schema-version-dv.xml");
    try (SolrQueryRequest r = req()) {
      SchemaField v = r.getCore().getUpdateHandler().getUpdateLog().getVersionInfo().getVersionField();
      assertNotNull(v);
      assertFalse(v.indexed());
      assertTrue(v.hasDocValues());
      
      testMaxVersionLogic(r);
    } finally {
      deleteCore();
    }
  }

  protected void testMaxVersionLogic(SolrQueryRequest req) throws Exception {
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();
    ulog.init(uhandler, req.getCore());

    clearIndex();
    assertU(commit());

    // index the first doc
    String docId = Integer.toString(1);
    BytesRef idBytes = new BytesRef(docId);
    assertU(adoc("id", docId));
    assertU(commit());

    // max from the ulog should not be 0 or null
    Long maxVersionFromUlog = ulog.getMaxVersionFromIndex();
    assertNotNull(maxVersionFromUlog);
    assertTrue(maxVersionFromUlog != 0L);

    VersionInfo vInfo = ulog.getVersionInfo();
    try (SolrQueryRequest newReq = req()) {
      // max version direct from the index should not be null, and should match what ulog reports
      // (since doc is committed)
      Long vInfoMax = vInfo.getMaxVersionFromIndex(newReq.getSearcher());
      assertNotNull(vInfoMax);
      assertEquals(maxVersionFromUlog, vInfoMax);
    }
    
    // max version from ulog (and index) should be exactly the same as our single committed doc
    Long version = vInfo.getVersionFromIndex(idBytes);
    assertNotNull("version info should not be null for test doc: "+docId, version);
    assertEquals(maxVersionFromUlog, version);

    int bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
    VersionBucket bucket = vInfo.bucket(bucketHash);
    assertEquals(bucket.highest, version.longValue());

    // send 2nd doc ... BUT DO NOT COMMIT
    docId = Integer.toString(2);
    idBytes = new BytesRef(docId);
    assertU(adoc("id", docId));
    
    try (SolrQueryRequest newReq = req()) {
      // max version direct from the index should not be null, and should still match what ulog
      // previously reported (since new doc is un-committed)
      Long vInfoMax = vInfo.getMaxVersionFromIndex(newReq.getSearcher());
      assertNotNull(vInfoMax);
      assertEquals(maxVersionFromUlog, vInfoMax);
    }
    
    maxVersionFromUlog = ulog.getMaxVersionFromIndex();
    assertNotNull(maxVersionFromUlog);
    assertTrue("max version in ulog should have increased since our last committed doc: " +
               version + " ?< " + maxVersionFromUlog,
               version < maxVersionFromUlog.longValue());

    version = vInfo.getVersionFromIndex(idBytes);
    assertNull("version info should be null for uncommited test doc: "+docId, version);

    Long versionFromTLog = ulog.lookupVersion(idBytes);
    assertNotNull("version from tlog should be non-null for uncommited test doc: "+docId, versionFromTLog);

    // now commit that 2nd doc
    assertU(commit());
    try (SolrQueryRequest newReq = req()) {
      // max version direct from the index should match the new doc we just committed
      Long vInfoMax = vInfo.getMaxVersionFromIndex(newReq.getSearcher());
      assertEquals(versionFromTLog, vInfoMax);
    }
    assertEquals("committing doc should not have changed version from ulog",
                 versionFromTLog, ulog.lookupVersion(idBytes));
    Long versionFromIndex = version = vInfo.getVersionFromIndex(idBytes);
    assertNotNull("version from index should be non-null for commited test doc: "+docId, versionFromIndex);
    assertEquals("version from tlog and version from index should be the same",
                 versionFromTLog, versionFromIndex);
    
    bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
    bucket = vInfo.bucket(bucketHash);
    assertEquals(bucket.highest, version.longValue());

    // reload the core, which should reset the max
    CoreContainer coreContainer = req.getCore().getCoreContainer();
    coreContainer.reload(req.getCore().getName());
    maxVersionFromUlog = ulog.getMaxVersionFromIndex();
    assertEquals("after reload, max version from ulog should be equal to version of last doc added",
                 maxVersionFromUlog, versionFromIndex);

    // one more doc after reload
    docId = Integer.toString(3);
    idBytes = new BytesRef(docId);
    assertU(adoc("id", docId));
    assertU(commit());

    maxVersionFromUlog = ulog.getMaxVersionFromIndex();
    assertNotNull(maxVersionFromUlog);
    assertTrue(maxVersionFromUlog != 0L);

    vInfo = ulog.getVersionInfo();
    try (SolrQueryRequest newReq = req()) {
      // max version direct from the index should not be null, and should match what ulog reports
      // (since doc is committed)
      Long vInfoMax = vInfo.getMaxVersionFromIndex(newReq.getSearcher());
      assertNotNull(vInfoMax);
      assertEquals(maxVersionFromUlog, vInfoMax);
    }
    version = vInfo.getVersionFromIndex(idBytes);
    assertNotNull("version info should not be null for test doc: "+docId, version);
    assertEquals(maxVersionFromUlog, version);

    bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
    bucket = vInfo.bucket(bucketHash);
    assertEquals(bucket.highest, version.longValue());
  }
}
