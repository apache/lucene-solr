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

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.http.util.Args;

/**
 * JWT principal that contains username, token, claims and a list of roles the user has, 
 * so one can keep track of user-role mappings in an Identity Server external to Solr and 
 * pass the information to Solr in a signed JWT token. The role information can then be used to authorize
 * requests without the need to maintain or lookup what roles each user belongs to.
 */
public class JWTPrincipalWithUserRoles extends JWTPrincipal implements VerifiedUserRoles {
  private final Set<String> roles;

  public JWTPrincipalWithUserRoles(final String username, String token, Map<String,Object> claims, Set<String> roles) {
    super(username, token, claims);
    Args.notNull(roles, "User roles");
    this.roles = roles;
  }

  /**
   * Gets the list of roles
   */
  @Override
  public Set<String> getVerifiedRoles() {
    return roles;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JWTPrincipalWithUserRoles))
      return false;
    JWTPrincipalWithUserRoles that = (JWTPrincipalWithUserRoles) o;
    return super.equals(o) && roles.equals(that.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, token, claims, roles);
  }

  @Override
  public String toString() {
    return "JWTPrincipalWithUserRoles{" +
        "username='" + username + '\'' +
        ", token='" + "*****" + '\'' +
        ", claims=" + claims +
        ", roles=" + roles +
        '}';
  }
}
