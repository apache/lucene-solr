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
import java.util.Map;
import java.util.Objects;

import org.apache.http.util.Args;

/**
 * Principal object that carries JWT token and claims for authenticated user.
 */
public class JWTPrincipal implements Principal {
  final String username;
  String token;
  Map<String,Object> claims;

  /**
   * User principal with user name as well as one or more roles that he/she belong to
   * @param username string with user name for user
   * @param token compact string representation of JWT token
   * @param claims list of verified JWT claims as a map
   */
  public JWTPrincipal(final String username, String token, Map<String,Object> claims) {
    super();
    Args.notNull(username, "User name");
    Args.notNull(token, "JWT token");
    Args.notNull(claims, "JWT claims");
    this.token = token;
    this.claims = claims;
    this.username = username;
  }

  @Override
  public String getName() {
    return this.username;
  }

  public String getToken() {
    return token;
  }

  public Map<String, Object> getClaims() {
    return claims;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JWTPrincipal that = (JWTPrincipal) o;
    return Objects.equals(username, that.username) &&
        Objects.equals(token, that.token) &&
        Objects.equals(claims, that.claims);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, token, claims);
  }

  @Override
  public String toString() {
    return "JWTPrincipal{" +
        "username='" + username + '\'' +
        ", token='" + "*****" + '\'' +
        ", claims=" + claims +
        '}';
  }
}
