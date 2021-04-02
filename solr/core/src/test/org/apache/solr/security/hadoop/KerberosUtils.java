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
package org.apache.solr.security.hadoop;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.solr.cloud.KerberosTestServices;

/**
 * A utility class which provides common functionality required to test kerberos integration.
 */
public class KerberosUtils {
  /**
   * This method sets up Hadoop mini-kdc along with relevant Kerberos configuration files
   * (e.g. jaas.conf) as well as system properties.
   *
   * @param baseDir The directory path which should be used by the Hadoop mini-kdc
   * @return An instance of {@link KerberosTestServices}
   * @throws Exception in case of errors.
   */
  static KerberosTestServices setupMiniKdc(Path baseDir) throws Exception {
    System.setProperty("solr.jaas.debug", "true");
    Path kdcDir = baseDir.resolve("minikdc");
    String solrClientPrincipal = "solr";
    String solrAltClientPrincipal = "solr_alt"; // An alternate principal that can be handled differently by authz tests
    File keytabFile = kdcDir.resolve("keytabs").toFile();
    KerberosTestServices tmp = KerberosTestServices.builder()
            .withKdc(kdcDir.toFile())
            .withJaasConfiguration(solrClientPrincipal, keytabFile, "SolrClient")
            .build();
    String solrServerPrincipal = "HTTP/127.0.0.1";
    tmp.start();
    tmp.getKdc().createPrincipal(keytabFile, solrServerPrincipal, solrAltClientPrincipal, solrClientPrincipal);

    String appName = "SolrClient";
    String jaas = appName + " {\n"
            + " com.sun.security.auth.module.Krb5LoginModule required\n"
            + " useKeyTab=true\n"
            + " keyTab=\"" + keytabFile.getAbsolutePath() + "\"\n"
            + " storeKey=true\n"
            + " useTicketCache=false\n"
            + " doNotPrompt=true\n"
            + " debug=true\n"
            + " principal=\"" + solrClientPrincipal + "\";\n"
            + "};";

    Path jaasFile = kdcDir.resolve("jaas-client.conf");
    try (OutputStream os = Files.newOutputStream(jaasFile)) {
      os.write(jaas.getBytes(StandardCharsets.UTF_8));
    }
    System.setProperty("java.security.auth.login.config", jaasFile.toString());
    System.setProperty("solr.kerberos.jaas.appname", appName);

    System.setProperty("solr.kerberos.principal", solrServerPrincipal);
    System.setProperty("solr.kerberos.keytab", keytabFile.getAbsolutePath());
    // Extracts 127.0.0.1 from HTTP/127.0.0.1@EXAMPLE.COM
    System.setProperty("solr.kerberos.name.rules", "RULE:[1:$1@$0](.*EXAMPLE.COM)s/@.*//"
            + "\nRULE:[2:$2@$0](.*EXAMPLE.COM)s/@.*//"
            + "\nDEFAULT"
    );

    return tmp;
  }

  /**
   * This method stops the Hadoop mini-kdc instance as well as cleanup relevant Java system properties.
   *
   * @param kerberosTestServices An instance of Hadoop mini-kdc
   */
  public static void cleanupMiniKdc(KerberosTestServices kerberosTestServices) {
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("solr.kerberos.principal");
    System.clearProperty("solr.kerberos.keytab");
    System.clearProperty("solr.kerberos.name.rules");
    System.clearProperty("solr.jaas.debug");
    if (kerberosTestServices != null) {
      kerberosTestServices.stop();
    }
  }
}
