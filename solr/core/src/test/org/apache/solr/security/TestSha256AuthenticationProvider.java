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
package org.apache.solr.security;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.CommandOperation;

import static java.util.Collections.singletonMap;

public class TestSha256AuthenticationProvider extends SolrTestCaseJ4 {
  public void testAuthenticate(){
    Sha256AuthenticationProvider zkAuthenticationProvider = new Sha256AuthenticationProvider();
    zkAuthenticationProvider.init(Collections.emptyMap());

    String pwd = "My#$Password";
    String user = "noble";
    @SuppressWarnings({"rawtypes"})
    Map latestConf = new LinkedHashMap<>();
    Map<String, Object> params = singletonMap(user, pwd);
    @SuppressWarnings({"unchecked"})
    Map<String, Object> result = zkAuthenticationProvider.edit(latestConf,
        Collections.singletonList(new CommandOperation("set-user",params )));
    zkAuthenticationProvider = new Sha256AuthenticationProvider();
    zkAuthenticationProvider.init(result);

    assertTrue(zkAuthenticationProvider.authenticate(user, pwd));
    assertFalse(zkAuthenticationProvider.authenticate(user, "WrongPassword"));
    assertFalse(zkAuthenticationProvider.authenticate("unknownuser", "WrongPassword"));

  }

  @SuppressWarnings({"unchecked"})
  public void testBasicAuthCommands() throws IOException {
    try (BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin()) {
      basicAuthPlugin.init(Collections.emptyMap());

      @SuppressWarnings({"rawtypes"})
      Map latestConf = new LinkedHashMap<>();

      CommandOperation blockUnknown = new CommandOperation("set-property", singletonMap("blockUnknown", true));
      basicAuthPlugin.edit(latestConf, Collections.singletonList(blockUnknown));
      assertEquals(Boolean.TRUE, latestConf.get("blockUnknown"));
      basicAuthPlugin.init(latestConf);
      assertTrue(basicAuthPlugin.getBlockUnknown());
      blockUnknown = new CommandOperation("set-property", singletonMap("blockUnknown", false));
      basicAuthPlugin.edit(latestConf, Collections.singletonList(blockUnknown));
      assertEquals(Boolean.FALSE, latestConf.get("blockUnknown"));
      basicAuthPlugin.init(latestConf);
      assertFalse(basicAuthPlugin.getBlockUnknown());
    }
  }
}
