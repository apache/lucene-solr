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

package org.apache.solr.client.solrj.request;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class TestLukeRequest extends SolrTestCaseJ4 {
  @Test
  public void testSkipsIncludeIndexFieldFlagsParamWhenNotSpecified() {
    final LukeRequest req = new LukeRequest();
    final SolrParams params = req.getParams();

    assertTrue("Expected the request to omit the 'includeIndexFieldFlags' param", params.get("includeIndexFieldFlags") == null);
  }

  @Test
  public void testContainsIncludeIndexFieldFlagsParamWhenSpecified() {
    LukeRequest req = new LukeRequest();
    req.setIncludeIndexFieldFlags(true);
    SolrParams params = req.getParams();

    assertEquals("true", params.get("includeIndexFieldFlags"));

    req = new LukeRequest();
    req.setIncludeIndexFieldFlags(false);
    params = req.getParams();
    assertEquals("false", params.get("includeIndexFieldFlags"));
  }
}
