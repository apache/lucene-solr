package org.apache.solr.cloud;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.hadoop.minikdc.MiniKdc;

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

public class KerberosTestUtil {

  /**
   * Returns a MiniKdc that can be used for creating kerberos principals
   * and keytabs.  Caller is responsible for starting/stopping the kdc.
   */
  public static MiniKdc getKdc(File workDir) throws Exception {
    Properties conf = MiniKdc.createConf();
    return new MiniKdc(conf, workDir);
  }

  /**
   * Programmatic version of a jaas.conf file suitable for connecting
   * to a SASL-configured zookeeper.
   */
  public static class JaasConfiguration extends Configuration {

    private static AppConfigurationEntry[] clientEntry;
    private static AppConfigurationEntry[] serverEntry;

    /**
     * Add an entry to the jaas configuration with the passed in name,
     * principal, and keytab. The other necessary options will be set for you.
     *
     * @param clientPrincipal The principal of the client
     * @param clientKeytab The location of the keytab with the clientPrincipal
     * @param serverPrincipal The principal of the server
     * @param serverKeytab The location of the keytab with the serverPrincipal
     */
    public JaasConfiguration(String clientPrincipal, File clientKeytab,
        String serverPrincipal, File serverKeytab) {
      Map<String, String> clientOptions = new HashMap<String, String>();
      clientOptions.put("principal", clientPrincipal);
      clientOptions.put("keyTab", clientKeytab.getAbsolutePath());
      clientOptions.put("useKeyTab", "true");
      clientOptions.put("storeKey", "true");
      clientOptions.put("useTicketCache", "false");
      clientOptions.put("refreshKrb5Config", "true");
      String jaasProp = System.getProperty("solr.jaas.debug");
      if (jaasProp != null && "true".equalsIgnoreCase(jaasProp)) {
        clientOptions.put("debug", "true");
      }
      clientEntry = new AppConfigurationEntry[]{
        new AppConfigurationEntry(getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        clientOptions)};
      Map<String, String> serverOptions = new HashMap<String, String>(clientOptions);
      serverOptions.put("principal", serverPrincipal);
      serverOptions.put("keytab", serverKeytab.getAbsolutePath());
      serverEntry =  new AppConfigurationEntry[]{
        new AppConfigurationEntry(getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        serverOptions)};
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      if ("Client".equals(name)) {
        return clientEntry;
      } else if ("Server".equals(name)) {
        return serverEntry;
      }
      return null;
    }

    private String getKrb5LoginModuleName() {
      String krb5LoginModuleName;
      if (System.getProperty("java.vendor").contains("IBM")) {
        krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
      } else {
        krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
      }
      return krb5LoginModuleName;
    }
  }
}
