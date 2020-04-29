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

import org.apache.http.HttpHeaders;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.security.cert.X509Certificate;
import java.util.Map;

/**
 * An authentication plugin that sets principal based on the certificate subject
 */
public class CertAuthPlugin extends AuthenticationPlugin {
    @Override
    public void init(Map<String, Object> pluginConfig) {

    }

    @Override
    public boolean doAuthenticate(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws Exception {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (certs == null || certs.length == 0) {
            numMissingCredentials.inc();
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Certificate");
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "require certificate");
            return false;
        }

        HttpServletRequest wrapped = wrapWithPrincipal(request, certs[0].getSubjectX500Principal());
        numAuthenticated.inc();
        filterChain.doFilter(wrapped, response);
        return true;
    }
}
