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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rule Based Authz plugin implementation which reads user roles from the request. This requires
 * a Principal implementing VerifiedUserRoles interface, e.g. JWTAuthenticationPlugin
 */
public class ExternalRoleRuleBasedAuthorizationPlugin extends RuleBasedAuthorizationPluginBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void init(Map<String, Object> initInfo) {
    super.init(initInfo);
    if (initInfo.containsKey("user-role")) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Configuration should not contain 'user-role' mappings");
    }
  }

  /**
   * Pulls roles from the Principal
   * @param principal the user Principal which should contain roles
   * @return set of roles as strings
   */
  @Override
  public Set<String> getUserRoles(Principal principal) {
    if(principal instanceof VerifiedUserRoles) {
      return ((VerifiedUserRoles) principal).getVerifiedRoles();
    } else {
      return Collections.emptySet();
    }
  }
}
