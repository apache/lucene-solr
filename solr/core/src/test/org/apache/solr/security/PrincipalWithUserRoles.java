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
import java.util.Objects;
import java.util.Set;

/**
 * Type of Principal object that can contain also a list of roles the user has.
 * One use case can be to keep track of user-role mappings in an Identity Server
 * external to Solr and pass the information to Solr in a signed JWT token or in 
 * another secure manner. The role information can then be used to authorize
 * requests without the need to maintain or lookup what roles each user belongs to. 
 */
public class PrincipalWithUserRoles implements Principal, VerifiedUserRoles {
  private final String username;

  private final Set<String> roles;

  /**
   * User principal with user name as well as one or more roles that he/she belong to
   * @param username string with user name for user
   * @param roles a set of roles that we know this user belongs to, or empty list for no roles
   */
  public PrincipalWithUserRoles(final String username, Set<String> roles) {
    super();
    Objects.requireNonNull(username, "User name was null");
    Objects.requireNonNull(roles, "User roles was null");
    this.username = username;
    this.roles = roles;
  }

  /**
   * Returns the name of this principal.
   *
   * @return the name of this principal.
   */
  @Override
  public String getName() {
    return this.username;
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PrincipalWithUserRoles that = (PrincipalWithUserRoles) o;

    if (!username.equals(that.username)) return false;
    return roles.equals(that.roles);
  }

  @Override
  public int hashCode() {
    int result = username.hashCode();
    result = 31 * result + roles.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "PrincipalWithUserRoles{" +
        "username='" + username + '\'' +
        ", roles=" + roles +
        '}';
  }
}
