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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.EventParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQuerySenderListener extends SolrTestCaseJ4 {

  // number of instances configured in the solrconfig.xml
  private static final int EXPECTED_MOCK_LISTENER_INSTANCES = 4;

  private static int preInitMockListenerCount = 0;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // record current value prior to core initialization
    // so we can verify the correct number of instances later
    // NOTE: this won't work properly if concurrent tests run
    // in the same VM
    preInitMockListenerCount = MockEventListener.getCreateCount();

    initCore("solrconfig-querysender.xml","schema.xml");

  }

  public void testListenerCreationCounts() {
    h.getCore();

    assertEquals("Unexpected number of listeners created",
                 EXPECTED_MOCK_LISTENER_INSTANCES, 
                 MockEventListener.getCreateCount() - preInitMockListenerCount);
  }

  @Test
  public void testRequestHandlerRegistry() {
    // property values defined in build.xml
    SolrCore core = h.getCore();

    assertEquals( 2, core.firstSearcherListeners.size() );
    assertEquals( 2, core.newSearcherListeners.size() );
  }

  @Test
  public void testSearcherEvents() throws Exception {
    SolrCore core = h.getCore();
    SolrEventListener newSearcherListener = core.newSearcherListeners.get(0);
    assertTrue("Not an instance of QuerySenderListener", newSearcherListener instanceof QuerySenderListener);
    QuerySenderListener qsl = (QuerySenderListener) newSearcherListener;

    h.getCore().withSearcher(currentSearcher -> {
      qsl.newSearcher(currentSearcher, null);//test new Searcher

      MockQuerySenderListenerReqHandler mock = (MockQuerySenderListenerReqHandler) core.getRequestHandler("/mock");
      assertNotNull("Mock is null", mock);

      {
        String evt = mock.req.getParams().get(EventParams.EVENT);
        assertNotNull("Event is null", evt);
        assertTrue(evt + " is not equal to " + EventParams.FIRST_SEARCHER, evt.equals(EventParams.FIRST_SEARCHER) == true);

        assertU(adoc("id", "1"));
        assertU(commit());
      }

      h.getCore().withSearcher(newSearcher -> {
        String evt = mock.req.getParams().get(EventParams.EVENT);
        assertNotNull("Event is null", evt);
        assertTrue(evt + " is not equal to " + EventParams.NEW_SEARCHER, evt.equals(EventParams.NEW_SEARCHER) == true);
        return null;
      });

      return null;
    });

  }

}

