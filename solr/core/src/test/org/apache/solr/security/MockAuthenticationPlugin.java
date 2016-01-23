package org.apache.solr.security;

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


import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.function.Predicate;

public class MockAuthenticationPlugin extends AuthenticationPlugin {
  static Predicate<ServletRequest> predicate;

  @Override
  public void init(Map<String, Object> pluginConfig) {
  }

  @Override
  public void doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
    String user = null;
    if (predicate != null) {
      if (predicate.test(request)) {
        user = (String) request.getAttribute(Principal.class.getName());
        request.removeAttribute(Principal.class.getName());
      }
    }
    forward(user, request, response, filterChain);
  }


  @Override
  public void close() throws IOException {

  }
}
