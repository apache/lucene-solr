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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.x500.X500Principal;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.security.cert.X509Certificate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CertAuthPluginTest extends SolrTestCaseJ4 {
    private CertAuthPlugin plugin;

    @BeforeClass
    public static void setupMockito() {
        SolrTestCaseJ4.assumeWorkingMockito();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        plugin = new CertAuthPlugin();
    }

    @Test
    public void testAuthenticateOk() throws Exception {
        X500Principal principal = new X500Principal("CN=NAME");
        X509Certificate certificate = mock(X509Certificate.class);
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(certificate.getSubjectX500Principal()).thenReturn(principal);
        when(request.getAttribute(any())).thenReturn(new X509Certificate[] { certificate });

        FilterChain chain = (req, rsp) -> assertEquals(principal, ((HttpServletRequest) req).getUserPrincipal());
        assertTrue(plugin.doAuthenticate(request, null, chain));

        assertEquals(1, plugin.numAuthenticated.getCount());
    }

    @Test
    public void testAuthenticateMissing() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute(any())).thenReturn(null);

        HttpServletResponse response = mock(HttpServletResponse.class);

        assertFalse(plugin.doAuthenticate(request, response, null));
        verify(response).sendError(eq(401), anyString());

        assertEquals(1, plugin.numMissingCredentials.getCount());
    }
}
