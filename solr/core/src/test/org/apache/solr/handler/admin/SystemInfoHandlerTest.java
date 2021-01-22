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
package org.apache.solr.handler.admin;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;

import com.codahale.metrics.Gauge;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.JWTPrincipal;
import org.apache.solr.security.MockAuthenticationPlugin;
import org.apache.solr.security.MockAuthorizationPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPluginBase;
import org.apache.solr.util.stats.MetricUtils;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


public class SystemInfoHandlerTest extends SolrTestCase {

  public void testMagickGetter() throws Exception {

    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    // make one directly
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    info.add( "name", os.getName() );
    info.add( "version", os.getVersion() );
    info.add( "arch", os.getArch() );

    // make another using MetricUtils.addMXBeanMetrics()
    SimpleOrderedMap<Object> info2 = new SimpleOrderedMap<>();
    MetricUtils.addMXBeanMetrics( os, OperatingSystemMXBean.class, null, (k, v) -> {
      info2.add(k, ((Gauge)v).getValue());
    } );

    // make sure they got the same thing
    for (String p : Arrays.asList("name", "version", "arch")) {
      assertEquals(info.get(p), info2.get(p));
    }
  }

  private static final String userName = "foobar";

  public void testGetSecurityInfoAuthorizationPlugin() throws Exception {
    final AuthorizationPlugin authorizationPlugin = new MockAuthorizationPlugin();
    doTestGetSecurityInfo(authorizationPlugin);
  }

  public void testGetSecurityInfoRuleBasedAuthorizationPlugin() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    final RuleBasedAuthorizationPluginBase ruleBasedAuthorizationPlugin = Mockito.mock(RuleBasedAuthorizationPlugin.class);
    Mockito.doReturn(Collections.EMPTY_SET).when(ruleBasedAuthorizationPlugin).getUserRoles(ArgumentMatchers.any(Principal.class));
    doTestGetSecurityInfo(ruleBasedAuthorizationPlugin);
  }

  private static void doTestGetSecurityInfo(AuthorizationPlugin authorizationPlugin) throws Exception {
    final AuthenticationPlugin authenticationPlugin = new MockAuthenticationPlugin() {
      @Override
      public String getName() {
        return "mock authentication plugin name";
      }
    };
    doTestGetSecurityInfo(null, null);
    doTestGetSecurityInfo(authenticationPlugin, null);
    doTestGetSecurityInfo(null, authorizationPlugin);
    doTestGetSecurityInfo(authenticationPlugin, authorizationPlugin);
  }

  private static void doTestGetSecurityInfo(AuthenticationPlugin authenticationPlugin, AuthorizationPlugin authorizationPlugin) throws Exception {

    SolrTestCaseJ4.assumeWorkingMockito();

    final CoreContainer cc = Mockito.mock(CoreContainer.class);
    {
      Mockito.doReturn(authenticationPlugin).when(cc).getAuthenticationPlugin();
      Mockito.doReturn(authorizationPlugin).when(cc).getAuthorizationPlugin();
    }

    final SolrQueryRequest req = Mockito.mock(SolrQueryRequestBase.class);
    {
      final Principal principal = Mockito.mock(JWTPrincipal.class);
      Mockito.doReturn(userName).when(principal).getName();
      Mockito.doReturn(principal).when(req).getUserPrincipal();
    }

    final SimpleOrderedMap<Object> si = SystemInfoHandler.getSecurityInfo(cc, req);

    if (authenticationPlugin != null) {
      assertEquals(authenticationPlugin.getName(), si.remove("authenticationPlugin"));
    } else {
      assertNull(si.remove("authenticationPlugin"));
    }

    if (authorizationPlugin != null) {
      assertEquals(authorizationPlugin.getClass().getName(), si.remove("authorizationPlugin"));
      if (authorizationPlugin instanceof RuleBasedAuthorizationPluginBase) {
        assertNotNull(si.remove("roles"));
      } else {
        assertNull(si.remove("roles"));
      }
    } else {
      assertNull(si.remove("authorizationPlugin"));
    }

    assertEquals(userName, si.remove("username"));

    assertEquals("Unexpected additional info: " + si, 0, si.size());
  }

}
