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
package org.apache.solr.core;

import java.util.Collections;
import java.util.Map;

import org.apache.solr.SolrTestCase;

import static org.apache.solr.core.ConfigOverlay.isEditableProp;

public class TestConfigOverlay extends SolrTestCase {

  public void testPaths() {
    assertTrue(isEditableProp("updateHandler/autoCommit/maxDocs", true, null));
    assertTrue(isEditableProp("updateHandler/autoCommit/maxTime", true, null));
    assertTrue(isEditableProp("updateHandler/autoCommit/openSearcher", true, null));
    assertTrue(isEditableProp("updateHandler/autoSoftCommit/maxDocs", true, null));
    assertTrue(isEditableProp("updateHandler/autoSoftCommit/maxTime", true, null));
    assertTrue(isEditableProp("updateHandler/commitWithin/softCommit", true, null));
    assertTrue(isEditableProp("updateHandler/indexWriter/closeWaitsForMerges", true, null));

    assertTrue(isEditableProp("updateHandler.autoCommit.maxDocs", false, null));
    assertTrue(isEditableProp("updateHandler.autoCommit.maxTime", false, null));
    assertTrue(isEditableProp("updateHandler.autoCommit.openSearcher", false, null));
    assertTrue(isEditableProp("updateHandler.autoSoftCommit.maxDocs", false, null));
    assertTrue(isEditableProp("updateHandler.autoSoftCommit.maxTime", false, null));
    assertTrue(isEditableProp("updateHandler.commitWithin.softCommit", false, null));
    assertTrue(isEditableProp("updateHandler.indexWriter.closeWaitsForMerges", false, null));
    assertTrue(isEditableProp("query.useFilterForSortedQuery", false, null));
    assertTrue(isEditableProp("query.queryResultWindowSize", false, null));
    assertTrue(isEditableProp("query.queryResultMaxDocsCached", false, null));
    assertTrue(isEditableProp("query.enableLazyFieldLoading", false, null));
    assertTrue(isEditableProp("query.boolTofilterOptimizer", false, null));

    assertTrue(isEditableProp("requestDispatcher.requestParsers.multipartUploadLimitInKB", false, null));
    assertTrue(isEditableProp("requestDispatcher.requestParsers.formdataUploadLimitInKB", false, null));
    assertTrue(isEditableProp("requestDispatcher.requestParsers.enableRemoteStreaming", false, null));
    assertTrue(isEditableProp("requestDispatcher.requestParsers.enableStreamBody", false, null));
    assertTrue(isEditableProp("requestDispatcher.requestParsers.addHttpRequestToContext", false, null));

    assertTrue(isEditableProp("requestDispatcher.handleSelect", false, null));

    assertTrue(isEditableProp("query.filterCache.initialSize", false, null));
    assertFalse(isEditableProp("query.filterCache", false, null));
    assertTrue(isEditableProp("query/filterCache/@initialSize", true, null));
    assertFalse(isEditableProp("query/filterCache/@initialSize1", true, null));
  }

  public void testSetProperty(){
    @SuppressWarnings({"unchecked"})
    ConfigOverlay overlay = new ConfigOverlay(Collections.EMPTY_MAP,0);
    overlay = overlay.setProperty("query.filterCache.initialSize",100);
    assertEquals(100, overlay.getXPathProperty("query/filterCache/@initialSize"));
    Map<String, String> map = overlay.getEditableSubProperties("query/filterCache");
    assertNotNull(map);
    assertEquals(1,map.size());
    assertEquals(100,map.get("initialSize"));
  }


}
