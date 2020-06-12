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

import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.SecurityConfHandler.getMapValue;

/**
 * Original implementation of Rule Based Authz plugin which configures user/role
 * mapping in the security.json configuration
 */
public class RuleBasedAuthorizationPlugin extends RuleBasedAuthorizationPluginBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, Set<String>> usersVsRoles = new HashMap<>();

  @Override
  public void init(Map<String, Object> initInfo) {
    super.init(initInfo);
    Map<String, Object> map = getMapValue(initInfo, "user-role");
    for (Object o : map.entrySet()) {
      @SuppressWarnings({"rawtypes"})
      Map.Entry e = (Map.Entry) o;
      String roleName = (String) e.getKey();
      usersVsRoles.put(roleName, Permission.readValueAsSet(map, roleName));
    }
  }

  /**
   * Look up user's role from the explicit user-role mapping
   *
   * @param principal the user Principal from the request
   * @return set of roles as strings
   */
  @Override
  public Set<String> getUserRoles(Principal principal) {
    return usersVsRoles.get(principal.getName());
  }
}
