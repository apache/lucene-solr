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

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.commons.io.FileUtils;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.KerberosTestServices;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.restlet.data.ChallengeScheme;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

@CreateLdapServer(
    transports =
        {
            @CreateTransport(protocol = "LDAP", address = "localhost"),
        })
@CreateDS(allowAnonAccess = true,
    partitions = {
        @CreatePartition(
            name = "Test_Partition", suffix = TestSolrCloudWithHadoopAuthPlugin.LDAP_BASE_DN,
            contextEntry = @ContextEntry(
                entryLdif = "dn: " + TestSolrCloudWithHadoopAuthPlugin.LDAP_BASE_DN + " \n" +
                    "dc: example\n" +
                    "objectClass: top\n" +
                    "objectClass: domain\n\n"))})
@ApplyLdifs({
    "dn: uid=bjones," + TestSolrCloudWithHadoopAuthPlugin.LDAP_BASE_DN,
    "cn: Bob Jones",
    "sn: Jones",
    "objectClass: inetOrgPerson",
    "uid: bjones",
    "userPassword: p@ssw0rd"})
@ThreadLeakFilters(defaultFilters = true, filters = {
    LdapServerThreadLeakFilter.class // Required until DIRSERVER-2176 is fixed.
})
@SolrTestCaseJ4.SuppressSSL
public class TestSolrCloudWithHadoopAuthPlugin extends SolrCloudAuthTestCase {
  public static final String LDAP_BASE_DN = "dc=example,dc=com";

  protected static final int NUM_SERVERS = 4;
  protected static final int NUM_SHARDS = 2;
  protected static final int REPLICATION_FACTOR = 2;

  private static KerberosTestServices kerberosTestServices;

  @ClassRule
  public static CreateLdapServerRule classCreateLdapServerRule = new CreateLdapServerRule();

  @BeforeClass
  public static void setupClass() throws Exception {
    HdfsTestUtil.checkAssumptions();

    setupMiniKdc();

    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_multi_scheme_config.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withDefaultClusterProperty("useLegacyReplicaAssignment", "false")
        .configure();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("solr.kerberos.principal");
    System.clearProperty("solr.kerberos.keytab");
    System.clearProperty("solr.kerberos.name.rules");
    System.clearProperty("solr.jaas.debug");
    System.clearProperty("hostName");
    System.clearProperty("solr.ldap.providerurl");
    System.clearProperty("solr.ldap.basedn");

    if (kerberosTestServices != null) {
      kerberosTestServices.stop();
      kerberosTestServices = null;
    }
  }

  private static void setupMiniKdc() throws Exception {
    System.setProperty("solr.jaas.debug", "true");
    String kdcDir = createTempDir()+File.separator+"minikdc";
    String solrClientPrincipal = "solr";
    File keytabFile = new File(kdcDir, "keytabs");
    kerberosTestServices = KerberosTestServices.builder()
        .withKdc(new File(kdcDir))
        .withJaasConfiguration(solrClientPrincipal, keytabFile, "SolrClient")
        .build();
    String solrServerPrincipal = "HTTP/127.0.0.1";
    kerberosTestServices.start();
    kerberosTestServices.getKdc().createPrincipal(keytabFile, solrServerPrincipal, solrClientPrincipal);

    String jaas = "SolrClient {\n"
        + " com.sun.security.auth.module.Krb5LoginModule required\n"
        + " useKeyTab=true\n"
        + " keyTab=\"" + keytabFile.getAbsolutePath() + "\"\n"
        + " storeKey=true\n"
        + " useTicketCache=false\n"
        + " doNotPrompt=true\n"
        + " debug=true\n"
        + " principal=\"" + solrClientPrincipal + "\";\n"
        + "};";

    String jaasFilePath = kdcDir+File.separator+"jaas-client.conf";
    FileUtils.write(new File(jaasFilePath), jaas, StandardCharsets.UTF_8);
    System.setProperty("java.security.auth.login.config", jaasFilePath);
    System.setProperty("solr.kerberos.jaas.appname", "SolrClient"); // Get this app name from the jaas file

    System.setProperty("solr.kerberos.principal", solrServerPrincipal);
    System.setProperty("solr.kerberos.keytab", keytabFile.getAbsolutePath());
    // Extracts 127.0.0.1 from HTTP/127.0.0.1@EXAMPLE.COM
    System.setProperty("solr.kerberos.name.rules", "RULE:[1:$1@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nRULE:[2:$2@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nDEFAULT"
        );

    System.setProperty("solr.ldap.providerurl", "ldap://localhost:" + classCreateLdapServerRule.getLdapServer().getPort());
    System.setProperty("solr.ldap.basedn", LDAP_BASE_DN);
  }

  @Test
  public void testBasics() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  protected void testCollectionCreateSearchDelete() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "testkerberoscollection";

    // create collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1",
        NUM_SHARDS, REPLICATION_FACTOR);
    create.process(solrClient);
    // The metrics counter for wrong credentials here really just means  
    assertAuthMetricsMinimums(6, 3, 0, 3, 0, 0);

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    solrClient.add(collectionName, doc);
    solrClient.commit(collectionName);
    assertAuthMetricsMinimums(10, 5, 0, 5, 0, 0);

    // Run a query using LDAP authentication mode (with correct credentials).
    Map result = runQuery("bjones", "p@ssw0rd", collectionName);
    assertEquals(Long.valueOf(0), readNestedElement(result, "responseHeader", "status"));
    assertEquals(Long.valueOf(1), readNestedElement(result, "response", "numFound"));

    // Run a query using LDAP authentication mode (with invalid credentials).
    try {
      runQuery("bjones", "invalidpassword", collectionName);
      fail("The query with invalid LDAP credentials must fail.");

    } catch (ResourceException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ex.getStatus().getCode());
    }

    CollectionAdminRequest.Delete deleteReq = CollectionAdminRequest.deleteCollection(collectionName);
    deleteReq.process(solrClient);
    AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName,
        solrClient.getZkStateReader(), true, true, 330);
    assertAuthMetricsMinimums(14, 8, 0, 6, 0, 0);
  }

  private <T> T runQuery(String userName, String password, String collectionName) throws ResourceException, IOException {
    String url = String.format(Locale.ROOT, "%s/%s/select?q=*:*",
        cluster.getJettySolrRunner(0).getBaseUrl().toString(), collectionName);
    ClientResource resource = new ClientResource(url);
    resource.setChallengeResponse(ChallengeScheme.HTTP_BASIC, userName, password);
    return deserialize(resource.get());
  }

  @SuppressWarnings("unchecked")
  private <T> T deserialize(Representation r) throws IOException {
    ByteArrayOutputStream str = new ByteArrayOutputStream();
    r.write(str);
    return (T) Utils.fromJSON(str.toByteArray());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private <T> T readNestedElement(Map object, String... fields) {
    Map t = object;
    int i = 0;

    while (i < fields.length - 1) {
      String field = fields[i];
      t = (Map) Objects.requireNonNull(t.get(field));
      i++;
    }

    return (T) Objects.requireNonNull(t.get(fields[fields.length - 1]));
  }
}
