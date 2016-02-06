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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class VersionInfoTest extends SolrTestCaseJ4 {

  @Test
  public void testMaxIndexedVersionFromIndex() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-version-indexed.xml");
    try {
      testMaxVersionLogic(req());
    } finally {
      deleteCore();
    }
  }

  @Test
  public void testMaxDocValuesVersionFromIndex() throws Exception {
    initCore("solrconfig-tlog.xml","schema-version-dv.xml");
    try {
      testMaxVersionLogic(req());
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
    assertU(adoc("id", docId));
    assertU(commit());

    // max from index should not be 0 or null
    Long maxVersionFromIndex = ulog.getMaxVersionFromIndex();
    assertNotNull(maxVersionFromIndex);
    assertTrue(maxVersionFromIndex != 0L);

    // version from index should be less than or equal the version of the first doc indexed
    VersionInfo vInfo = ulog.getVersionInfo();
    Long version = vInfo.getVersionFromIndex(new BytesRef(docId));
    assertNotNull("version info should not be null for test doc: "+docId, version);
    assertTrue("max version from index should be less than or equal to the version of first doc added, diff: "+
            (version - maxVersionFromIndex), maxVersionFromIndex <= version);

    BytesRef idBytes = new BytesRef(docId);
    int bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
    VersionBucket bucket = vInfo.bucket(bucketHash);
    assertTrue(bucket.highest == version.longValue());

    // send 2nd doc ...
    docId = Integer.toString(2);
    assertU(adoc("id", docId));
    assertU(commit());

    maxVersionFromIndex = ulog.getMaxVersionFromIndex();
    assertNotNull(maxVersionFromIndex);
    assertTrue(maxVersionFromIndex != 0L);

    vInfo = ulog.getVersionInfo();
    version = vInfo.getVersionFromIndex(new BytesRef(docId));
    assertNotNull("version info should not be null for test doc: "+docId, version);
    assertTrue("max version from index should be less than version of last doc added, diff: "+
            (version - maxVersionFromIndex), maxVersionFromIndex < version);

    idBytes = new BytesRef(docId);
    bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
    bucket = vInfo.bucket(bucketHash);
    assertTrue(bucket.highest == version.longValue());

    Long versionFromTLog = ulog.lookupVersion(idBytes);
    Long versionFromIndex = vInfo.getVersionFromIndex(idBytes);
    assertEquals("version from tlog and version from index should be the same",
        versionFromTLog, versionFromIndex);

    // reload the core, which should reset the max
    CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    coreContainer.reload(req.getCore().getName());
    maxVersionFromIndex = ulog.getMaxVersionFromIndex();
    assertEquals("max version from index should be equal to version of last doc added after reload",
        maxVersionFromIndex, version);

    // one more doc after reload
    docId = Integer.toString(3);
    assertU(adoc("id", docId));
    assertU(commit());

    maxVersionFromIndex = ulog.getMaxVersionFromIndex();
    assertNotNull(maxVersionFromIndex);
    assertTrue(maxVersionFromIndex != 0L);

    vInfo = ulog.getVersionInfo();
    version = vInfo.getVersionFromIndex(new BytesRef(docId));
    assertNotNull("version info should not be null for test doc: "+docId, version);
    assertTrue("max version from index should be less than version of last doc added, diff: "+
        (version - maxVersionFromIndex), maxVersionFromIndex < version);

    idBytes = new BytesRef(docId);
    bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
    bucket = vInfo.bucket(bucketHash);
    assertTrue(bucket.highest == version.longValue());
  }
}
