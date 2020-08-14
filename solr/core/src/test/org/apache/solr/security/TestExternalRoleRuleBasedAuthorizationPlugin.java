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

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.http.auth.BasicUserPrincipal;

/**
 * Tests {@link ExternalRoleRuleBasedAuthorizationPlugin} through simulating principals with roles attached
 */
public class TestExternalRoleRuleBasedAuthorizationPlugin extends BaseTestRuleBasedAuthorizationPlugin {
  private HashMap<String, Principal> principals;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    principals = new HashMap<>();
    setUserRoles("steve", "dev", "user");
    setUserRoles("tim", "dev", "admin");
    setUserRoles("joe", "user");
    setUserRoles("noble", "dev", "user");
  }

  protected void setUserRoles(String user, String... roles) {
    principals.put(user, new PrincipalWithUserRoles(user, new HashSet<>(Arrays.asList(roles))));
  }

  @Override
  protected void setUserRole(String user, String role) {
    principals.put(user, new PrincipalWithUserRoles(user, Collections.singleton(role)));
  }

  @Override
  AuthorizationContext getMockContext(Map<String, Object> values) {
    return new MockAuthorizationContext(values) {
      @Override
      public Principal getUserPrincipal() {
        String userPrincipal = (String) values.get("userPrincipal");
        return userPrincipal == null ? null :
            principals.get(userPrincipal) != null ? principals.get(userPrincipal) :
                new BasicUserPrincipal(userPrincipal);
      }
    };
  }

  @Override
  protected RuleBasedAuthorizationPluginBase createPlugin() {
    return new ExternalRoleRuleBasedAuthorizationPlugin();
  }

  @Override
  protected void resetPermissionsAndRoles() {
    super.resetPermissionsAndRoles();
    rules.remove("user-role");
  }
}
