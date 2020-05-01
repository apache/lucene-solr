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
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockAuthorizationPlugin implements AuthorizationPlugin {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final HashSet<String> denyUsers = new HashSet<>();
  static final HashSet<String> protectedResources = new HashSet<>();
  static Predicate<AuthorizationContext> predicate;

  @Override
  public AuthorizationResponse authorize(AuthorizationContext context) {
    String uname = context.getUserPrincipal() == null ? null : context.getUserPrincipal().getName();
    if (predicate != null) {
      try {
        predicate.test(context);
        return new AuthorizationResponse(200);
      } catch (SolrException e) {
        return new AuthorizationResponse(e.code());
      }
    } else {
      if (!protectedResources.contains(context.getResource())) {
        return new AuthorizationResponse(200);
      }
      if (uname == null) uname = context.getParams().get("uname");
      log.info("User request: {}", uname);
      if (uname == null || denyUsers.contains(uname))
        return new AuthorizationResponse(403);
      else
        return new AuthorizationResponse(200);
    }
  }

  @Override
  public void init(Map<String, Object> initInfo) {
  }

  @Override
  public void close() throws IOException {

  }
}
