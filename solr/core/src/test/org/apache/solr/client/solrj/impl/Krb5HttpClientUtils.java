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
package org.apache.solr.client.solrj.impl;

import org.eclipse.jetty.client.HttpAuthenticationStore;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.client.util.SPNEGOAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.lang.invoke.MethodHandles;
import java.net.URI;

/**
 * All of this is a clone of Krb5HttpClientBuilder to hardcode the user-principal for a unit test
 */
public class Krb5HttpClientUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static Configuration jaasConfig = new Krb5HttpClientBuilder.SolrJaasConfiguration();


  public static void setup(Http2SolrClient http2Client, String principalName) {
    HttpAuthenticationStore authenticationStore = new HttpAuthenticationStore();
    authenticationStore.addAuthentication(createSPNEGOAuthentication(principalName));
    http2Client.getHttpClient().setAuthenticationStore(authenticationStore);
    http2Client.getProtocolHandlers().put(new WWWAuthenticationProtocolHandler(http2Client.getHttpClient()));
  }

  private static SPNEGOAuthentication createSPNEGOAuthentication(String principalName) {
    SPNEGOAuthentication authentication = new SPNEGOAuthentication(null){

      public boolean matches(String type, URI uri, String realm) {
        return this.getType().equals(type);
      }
    };
    String clientAppName = System.getProperty("solr.kerberos.jaas.appname", "Client");
    AppConfigurationEntry[] entries = jaasConfig.getAppConfigurationEntry(clientAppName);
    if (entries == null) {
      log.warn("Could not find login configuration entry for {}. SPNego authentication may not be successful.", (Object)clientAppName);
      return authentication;
    }
    if (entries.length != 1) {
      log.warn("Multiple login modules are specified in the configuration file");
      return authentication;
    }

    Krb5HttpClientBuilder.setAuthenticationOptions(authentication, entries[0].getOptions(), principalName);
    return authentication;
  }
}
