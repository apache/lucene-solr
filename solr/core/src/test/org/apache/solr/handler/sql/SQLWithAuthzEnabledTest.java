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

package org.apache.solr.handler.sql;

import java.io.IOException;
import java.util.Arrays;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

public class SQLWithAuthzEnabledTest extends SolrCloudTestCase {

  private static final String ADMIN_USER = "solr";
  private static final String SQL_USER = "sql";
  private static final String SAD_USER = "sad";
  private static final String PASS = "SolrRocks!!";
  private static final String collectionName = "testSQLWithAuthz";

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    final String SECURITY_JSON = Utils.toJSONString
        (makeMap("authorization",
            makeMap("class", RuleBasedAuthorizationPlugin.class.getName(),
                "user-role", makeMap(SQL_USER, "sql", ADMIN_USER, "admin", SAD_USER, "sad"),
                "permissions", Arrays.asList(
                    makeMap("name", "sql", "role", "sql", "path", "/sql", "collection", "*"),
                    makeMap("name", "export", "role", "sql", "path", "/export", "collection", "*"),
                    makeMap("name", "all", "role", "admin"))
            ),
            "authentication",
            makeMap("class", BasicAuthPlugin.class.getName(),
                "blockUnknown", true,
                "credentials", makeMap(
                    SAD_USER, getSaltedHashedValue(PASS),
                    SQL_USER, getSaltedHashedValue(PASS),
                    ADMIN_USER, getSaltedHashedValue(PASS)))));

    configureCluster(2)
        .addConfig("conf", configset("sql"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
  }

  private <T extends SolrRequest<? extends SolrResponse>> T doAsAdmin(T req) {
    req.setBasicAuthCredentials(ADMIN_USER, PASS);
    return req;
  }

  @Test
  public void testSqlAuthz() throws Exception {
    doAsAdmin(CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2, 0, 0)).process(cluster.getSolrClient());
    waitForState("Expected collection to be created with 1 shards and 2 replicas", collectionName, clusterShape(1, 2));

    doAsAdmin(new UpdateRequest()
        .add("id", "1")
        .add("id", "2")
        .add("id", "3")
        .add("id", "4")
        .add("id", "5")
        .add("id", "6")
        .add("id", "7")
        .add("id", "8")).commit(cluster.getSolrClient(), collectionName);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/sql");
    params.set("stmt", "select id from " + collectionName);
    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + collectionName;
    SolrStream solrStream = new SolrStream(baseUrl, params);
    solrStream.setCredentials(SAD_USER, PASS);

    // sad user is not authorized to access /sql endpoints
    expectThrows(IOException.class, () -> countTuples(solrStream));

    // sql user has access
    SolrStream solrStream2 = new SolrStream(baseUrl, params);
    solrStream2.setCredentials(SQL_USER, PASS);
    assertEquals(8, countTuples(solrStream2));
  }

  private int countTuples(TupleStream tupleStream) throws IOException {
    int count = 0;
    try {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read())
        count++;
    } finally {
      tupleStream.close();
    }
    return count;
  }
}
