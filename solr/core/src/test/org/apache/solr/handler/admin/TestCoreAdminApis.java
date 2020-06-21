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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;

import static org.apache.solr.common.util.Utils.fromJSONString;
import static org.mockito.Mockito.*;

public class TestCoreAdminApis extends SolrTestCaseJ4 {

  @SuppressWarnings({"unchecked"})
  public void testCalls() throws Exception {
    Map<String, Object[]> calls = new HashMap<>();
    CoreContainer mockCC = getCoreContainerMock(calls, new HashMap<>());

    ApiBag apiBag;
    try (CoreAdminHandler  coreAdminHandler = new CoreAdminHandler(mockCC)) {
       apiBag = new ApiBag(false);
       for (Api api : coreAdminHandler.getApis()) {
         apiBag.register(api, Collections.EMPTY_MAP);
       }
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
    params = calls.get("rename");
    assertEquals("core1" ,params[0]);
    assertEquals("core2" ,params[1]);

    TestCollectionAPIs.makeCall(apiBag, "/cores/core1", SolrRequest.METHOD.POST,
        "{unload:{deleteIndex : true}}", mockCC);
    params = calls.get("unload");
    assertEquals("core1" ,params[0]);
    assertEquals(Boolean.TRUE ,params[1]);
  }

  @SuppressWarnings({"unchecked"})
  public static CoreContainer getCoreContainerMock(final Map<String, Object[]> in,Map<String,Object> out ) {
    assumeWorkingMockito();
    
    CoreContainer mockCC = mock(CoreContainer.class);
    when(mockCC.create(any(String.class), any(Path.class) , any(Map.class), anyBoolean())).thenAnswer(invocationOnMock -> {
      in.put("create", invocationOnMock.getArguments());
      return null;
    });

    doAnswer(invocationOnMock -> {
      in.put("swap", invocationOnMock.getArguments());
      return null;
    }).when(mockCC).swap(any(String.class), any(String.class));

    doAnswer(invocationOnMock -> {
      in.put("rename", invocationOnMock.getArguments());
      return null;
    }).when(mockCC).rename(any(String.class), any(String.class));


    doAnswer(invocationOnMock -> {
      in.put("unload", invocationOnMock.getArguments());
      return null;
    }).when(mockCC).unload(any(String.class), anyBoolean(),
        anyBoolean(), anyBoolean());

    when(mockCC.getCoreRootDirectory()).thenReturn(Paths.get("coreroot"));
    when(mockCC.getContainerProperties()).thenReturn(new Properties());

    when(mockCC.getRequestHandlers()).thenAnswer(invocationOnMock -> out.get("getRequestHandlers"));
    return mockCC;
  }


}
