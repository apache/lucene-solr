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

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.auth.Credentials;

/**
 * Credentials implementation that holds on to a (JWT) token for later use in inter-node requests
 */
public class TokenCredentials implements Credentials {
    private Principal userPrincipal;

    public TokenCredentials(String token) {
        this.userPrincipal = new BasicUserPrincipal(token);
    }

    @Override
    public Principal getUserPrincipal() {
        return userPrincipal;
    }

    @Override
    public String getPassword() {
        return null;
    }
}