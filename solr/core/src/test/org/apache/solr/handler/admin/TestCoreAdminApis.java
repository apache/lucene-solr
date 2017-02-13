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

package org.apache.solr.handler.admin;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.easymock.EasyMock;
import org.junit.BeforeClass;

import static org.apache.solr.common.util.Utils.fromJSONString;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.getCurrentArguments;

public class TestCoreAdminApis extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeFalse("SOLR-9893: EasyMock does not work with Java 9", Constants.JRE_IS_MINIMUM_JAVA9);
  }

  public void testCalls() throws Exception {
    Map<String, Object[]> calls = new HashMap<>();
    CoreContainer mockCC = getCoreContainerMock(calls, new HashMap<>());

    CoreAdminHandler  coreAdminHandler = new CoreAdminHandler(mockCC);
    ApiBag apiBag = new ApiBag(false);
    for (Api api : coreAdminHandler.getApis()) {
      apiBag.register(api, Collections.EMPTY_MAP);
    }
    TestCollectionAPIs.makeCall(apiBag, "/cores", SolrRequest.METHOD.POST,
        "{create:{name: hello, instanceDir : someDir, schema: 'schema.xml'}}", mockCC);
    Object[] params = calls.get("create");
    assertEquals("hello" ,params[0]);
    assertEquals(fromJSONString("{schema : schema.xml}") ,params[2]);

    TestCollectionAPIs.makeCall(apiBag, "/cores/core1", SolrRequest.METHOD.POST,
        "{swap:{with: core2}}", mockCC);
    params = calls.get("swap");
    assertEquals("core1" ,params[0]);
    assertEquals("core2" ,params[1]);

    TestCollectionAPIs.makeCall(apiBag, "/cores/core1", SolrRequest.METHOD.POST,
        "{rename:{to: core2}}", mockCC);
    params = calls.get("swap");
    assertEquals("core1" ,params[0]);
    assertEquals("core2" ,params[1]);

    TestCollectionAPIs.makeCall(apiBag, "/cores/core1", SolrRequest.METHOD.POST,
        "{unload:{deleteIndex : true}}", mockCC);
    params = calls.get("unload");
    assertEquals("core1" ,params[0]);
    assertEquals(Boolean.TRUE ,params[1]);
  }

  public static CoreContainer getCoreContainerMock(final Map<String, Object[]> in,Map<String,Object> out ) {
    CoreContainer mockCC = EasyMock.createMock(CoreContainer.class);
    EasyMock.reset(mockCC);
    mockCC.create(anyObject(String.class), anyObject(Path.class) , anyObject(Map.class), anyBoolean());
    EasyMock.expectLastCall().andAnswer(() -> {
      in.put("create", getCurrentArguments());
      return null;
    }).anyTimes();
    mockCC.swap(anyObject(String.class), anyObject(String.class));
    EasyMock.expectLastCall().andAnswer(() -> {
      in.put("swap", getCurrentArguments());
      return null;
    }).anyTimes();

    mockCC.rename(anyObject(String.class), anyObject(String.class));
    EasyMock.expectLastCall().andAnswer(() -> {
      in.put("rename", getCurrentArguments());
      return null;
    }).anyTimes();

    mockCC.unload(anyObject(String.class), anyBoolean(),
        anyBoolean(), anyBoolean());
    EasyMock.expectLastCall().andAnswer(() -> {
      in.put("unload", getCurrentArguments());
      return null;
    }).anyTimes();

    mockCC.getCoreRootDirectory();
    EasyMock.expectLastCall().andAnswer(() -> Paths.get("coreroot")).anyTimes();
    mockCC.getContainerProperties();
    EasyMock.expectLastCall().andAnswer(() -> new Properties()).anyTimes();

    mockCC.getRequestHandlers();
    EasyMock.expectLastCall().andAnswer(() -> out.get("getRequestHandlers")).anyTimes();

    EasyMock.replay(mockCC);
    return mockCC;
  }


}
