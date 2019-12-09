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

package org.apache.solr.pkg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriterMap;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.filestore.TestDistribPackageStore;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_PKGS_PATH;
import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;
import static org.apache.solr.filestore.TestDistribPackageStore.readFile;
import static org.apache.solr.filestore.TestDistribPackageStore.waitForAllNodesHaveFile;

@LogLevel("org.apache.solr.pkg.PackageLoader=DEBUG;org.apache.solr.pkg.PackageAPI=DEBUG")
//@org.apache.lucene.util.LuceneTestCase.AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13822") // leaks files
public class TestPackages extends SolrCloudTestCase {

  @Test
  public void testPluginLoading() throws Exception {
    System.setProperty("enable.packages", "true");
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
    try {
      String FILE1 = "/mypkg/runtimelibs.jar";
      String FILE2 = "/mypkg/runtimelibs_v2.jar";
      String FILE3 = "/mypkg/runtimelibs_v3.jar";
      String URP1 = "/mypkg/testurpv1.jar";
      String URP2 = "/mypkg/testurpv2.jar";
      String COLLECTION_NAME = "testPluginLoadingColl";
      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);
      postFileAndWait(cluster, "runtimecode/runtimelibs.jar.bin", FILE1,
          "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==");

      postFileAndWait(cluster, "runtimecode/testurp_v1.jar.bin", URP1,
          "h6UmMzuPqu4hQFGLBMJh/6kDSEXpJlgLsQDXx0KuxXWkV5giilRP57K3towiJRh2J+rqihqIghNCi3YgzgUnWQ==");

      Package.AddVersion add = new Package.AddVersion();
      add.version = "1.0";
      add.pkg = "mypkg";
      add.files = Arrays.asList(new String[]{FILE1, URP1});
      V2Request req = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("add", add))
          .build();

      req.process(cluster.getSolrClient());


      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 2)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);

      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/package").
              withMethod(SolrRequest.METHOD.GET)
              .build().process(cluster.getSolrClient()),
          Utils.makeMap(
              ":result:packages:mypkg[0]:version", "1.0",
              ":result:packages:mypkg[0]:files[0]", FILE1
          ));

      String payload = "{\n" +
          "'create-requesthandler' : { 'name' : '/runtime', 'class': 'mypkg:org.apache.solr.core.RuntimeLibReqHandler' }," +
          "'create-searchcomponent' : { 'name' : 'get', 'class': 'mypkg:org.apache.solr.core.RuntimeLibSearchComponent'  }," +
          "'create-queryResponseWriter' : { 'name' : 'json1', 'class': 'mypkg:org.apache.solr.core.RuntimeLibResponseWriter' }" +
          "'create-updateProcessor' : { 'name' : 'myurp', 'class': 'mypkg:org.apache.solr.update.TestVersionedURP' }" +
          "}";
      cluster.getSolrClient().request(new ConfigRequest(payload) {
        @Override
        public String getCollection() {
          return COLLECTION_NAME;
        }
      });

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "updateProcessor", "myurp",
          "mypkg", "1.0" );

      UpdateRequest ur = new UpdateRequest();
      ur.add(new SolrInputDocument("id", "1"));
      ur.setParam("processor", "myurp");
      ur.process(cluster.getSolrClient(), COLLECTION_NAME);
      cluster.getSolrClient().commit(COLLECTION_NAME, true, true);

      QueryResponse result = cluster.getSolrClient()
          .query(COLLECTION_NAME, new SolrQuery( "id:1"));

      assertEquals("Version 1", result.getResults().get(0).getFieldValue("TestVersionedURP.Ver_s"));

      executeReq( "/" + COLLECTION_NAME + "/runtime?wt=javabin", cluster.getRandomJetty(random()),
          Utils.JAVABINCONSUMER,
          Utils.makeMap("class", "org.apache.solr.core.RuntimeLibReqHandler"));

      executeReq( "/" + COLLECTION_NAME + "/get?wt=json", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap("class", "org.apache.solr.core.RuntimeLibSearchComponent",
              "Version","1"));


      executeReq( "/" + COLLECTION_NAME + "/runtime?wt=json1", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap("wt", "org.apache.solr.core.RuntimeLibResponseWriter"));

      //now upload the second jar
      postFileAndWait(cluster, "runtimecode/runtimelibs_v2.jar.bin", FILE2,
          "j+Rflxi64tXdqosIhbusqi6GTwZq8znunC/dzwcWW0/dHlFGKDurOaE1Nz9FSPJuXbHkVLj638yZ0Lp1ssnoYA==");

      postFileAndWait(cluster, "runtimecode/testurp_v2.jar.bin", URP2,
          "P/ptFXRvQMd4oKPvadSpd+A9ffwY3gcex5GVFVRy3df0/OF8XT5my8rQz7FZva+2ORbWxdXS8NKwNrbPVHLGXw==");
      //add the version using package API
      add.version = "1.1";
      add.files = Arrays.asList(new String[]{FILE2,URP2});
      req.process(cluster.getSolrClient());

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "updateProcessor", "myurp",
          "mypkg", "1.1" );


      executeReq( "/" + COLLECTION_NAME + "/get?wt=json", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap(  "Version","2"));


      //now upload the third jar
      postFileAndWait(cluster, "runtimecode/runtimelibs_v3.jar.bin", FILE3,
          "a400n4T7FT+2gM0SC6+MfSOExjud8MkhTSFylhvwNjtWwUgKdPFn434Wv7Qc4QEqDVLhQoL3WqYtQmLPti0G4Q==");

      add.version = "2.1";
      add.files = Arrays.asList(new String[]{FILE3, URP2});
      req.process(cluster.getSolrClient());

      //now let's verify that the classes are updated
      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "2.1" );

      executeReq( "/" + COLLECTION_NAME + "/runtime?wt=json", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap("Version","2"));

      //insert a doc with urp
      ur = new UpdateRequest();
      ur.add(new SolrInputDocument("id", "2"));
      ur.setParam("processor", "myurp");
      ur.process(cluster.getSolrClient(), COLLECTION_NAME);
      cluster.getSolrClient().commit(COLLECTION_NAME, true, true);

      result = cluster.getSolrClient()
          .query(COLLECTION_NAME, new SolrQuery( "id:2"));

      assertEquals("Version 2", result.getResults().get(0).getFieldValue("TestVersionedURP.Ver_s"));


      Package.DelVersion delVersion = new Package.DelVersion();
      delVersion.pkg = "mypkg";
      delVersion.version = "1.0";
      V2Request delete = new V2Request.Builder("/cluster/package")
          .withMethod(SolrRequest.METHOD.POST)
          .forceV2(true)
          .withPayload(Collections.singletonMap("delete", delVersion))
          .build();
      delete.process(cluster.getSolrClient());

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "2.1" );

      // now remove the hughest version. So, it will roll back to the next highest one
      delVersion.version = "2.1";
      delete.process(cluster.getSolrClient());

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.1" );

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("collection", COLLECTION_NAME);
      new GenericSolrRequest(SolrRequest.METHOD.POST, "/config/params", params ){
        @Override
        public RequestWriter.ContentWriter getContentWriter(String expectedType) {
          return new RequestWriter.StringPayloadContentWriter("{set:{PKG_VERSIONS:{mypkg : '1.1'}}}",
              ClientUtils.TEXT_JSON);
        }
      }.process(cluster.getSolrClient()) ;

      add.version = "2.1";
      add.files = Arrays.asList(new String[]{FILE3, URP2});
      req.process(cluster.getSolrClient());

      //the collections mypkg is set to use version 1.1
      //so no upgrade

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.1" );

      new GenericSolrRequest(SolrRequest.METHOD.POST, "/config/params", params ){
        @Override
        public RequestWriter.ContentWriter getContentWriter(String expectedType) {
          return new RequestWriter.StringPayloadContentWriter("{set:{PKG_VERSIONS:{mypkg : '2.1'}}}",
              ClientUtils.TEXT_JSON);
        }
      }.process(cluster.getSolrClient()) ;

      //now, let's force every collection using 'mypkg' to refresh
      //so that it uses version 2.1
      new V2Request.Builder("/cluster/package")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{refresh : mypkg}")
          .forceV2(true)
          .build()
          .process(cluster.getSolrClient());


      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "2.1" );
      //we create a new node. This node does not have the packages. But it should download it from another node
      JettySolrRunner jetty = cluster.startJettySolrRunner();
      //create a new replica for this collection. it should end up
      CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, "shard1")
          .setNrtReplicas(1)
          .setNode(jetty.getNodeName())
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 5);
      waitForAllNodesHaveFile(cluster,FILE3,
          Utils.makeMap(":files:" + FILE3 + ":name", "runtimelibs_v3.jar"),
          false);

    } finally {
      cluster.shutdown();
    }

  }

  private void executeReq(String uri, JettySolrRunner jetty, Utils.InputStreamConsumer parser, Map expected) throws Exception {
    try(HttpSolrClient client = (HttpSolrClient) jetty.newClient()){
      TestDistribPackageStore.assertResponseValues(10,
          () -> {
            Object o = Utils.executeGET(client.getHttpClient(),
                jetty.getBaseUrl() + uri, parser);
            if(o instanceof NavigableObject) return (NavigableObject) o;
            if(o instanceof Map) return new MapWriterMap((Map) o);
            throw new RuntimeException("Unknown response");
          }, expected);

    }
  }

  private void verifyCmponent(SolrClient client, String COLLECTION_NAME,
  String componentType, String componentName, String pkg, String version) throws Exception {
    SolrParams params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
        WT, JAVABIN,
        "componentName", componentName,
        "meta", "true"));

    String s = "queryResponseWriter";
    GenericSolrRequest req1 = new GenericSolrRequest(SolrRequest.METHOD.GET,
        "/config/" + componentType, params);
    TestDistribPackageStore.assertResponseValues(10,
        client,
        req1, Utils.makeMap(
            ":config:" + componentType + ":" + componentName + ":_packageinfo_:package", pkg,
            ":config:" + componentType + ":" + componentName + ":_packageinfo_:version", version
        ));
  }

  @Test
  public void testAPI() throws Exception {
    System.setProperty("enable.packages", "true");
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
    try {
      String errPath = "/error/details[0]/errorMessages[0]";
      String FILE1 = "/mypkg/v.0.12/jar_a.jar";
      String FILE2 = "/mypkg/v.0.12/jar_b.jar";
      String FILE3 = "/mypkg/v.0.13/jar_a.jar";

      Package.AddVersion add = new Package.AddVersion();
      add.version = "0.12";
      add.pkg = "test_pkg";
      add.files = Arrays.asList(new String[]{FILE1, FILE2});
      V2Request req = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("add", add))
          .build();

      //the files is not yet there. The command should fail with error saying "No such file"
      expectError(req, cluster.getSolrClient(), errPath, "No such file :");


      //post the jar file. No signature is sent
      postFileAndWait(cluster, "runtimecode/runtimelibs.jar.bin", FILE1, null);


      add.files = Arrays.asList(new String[]{FILE1});
      expectError(req, cluster.getSolrClient(), errPath,
          FILE1 + " has no signature");
      //now we upload the keys
      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);
      //and upload the same file with a different name but it has proper signature
      postFileAndWait(cluster, "runtimecode/runtimelibs.jar.bin", FILE2,
          "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==");
      // with correct signature
      //after uploading the file, let's delete the keys to see if we get proper error message
      cluster.getZkClient().delete("/keys/exe/pub_key512.der", -1, true);
      add.files = Arrays.asList(new String[]{FILE2});
      expectError(req, cluster.getSolrClient(), errPath,
          "ZooKeeper does not have any public keys");

      //Now lets' put the keys back
      cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);

      //this time we have a file with proper signature, public keys are in ZK
      // so the add {} command should succeed
      req.process(cluster.getSolrClient());

      //Now verify the data in ZK
      TestDistribPackageStore.assertResponseValues(1,
          () -> new MapWriterMap((Map) Utils.fromJSON(cluster.getZkClient().getData(SOLR_PKGS_PATH,
              null, new Stat(), true))),
          Utils.makeMap(
              ":packages:test_pkg[0]:version", "0.12",
              ":packages:test_pkg[0]:files[0]", FILE1
          ));

      //post a new jar with a proper signature
      postFileAndWait(cluster, "runtimecode/runtimelibs_v2.jar.bin", FILE3,
          "j+Rflxi64tXdqosIhbusqi6GTwZq8znunC/dzwcWW0/dHlFGKDurOaE1Nz9FSPJuXbHkVLj638yZ0Lp1ssnoYA==");


      //this time we are adding the second version of the package (0.13)
      add.version = "0.13";
      add.pkg = "test_pkg";
      add.files = Arrays.asList(new String[]{FILE3});

      //this request should succeed
      req.process(cluster.getSolrClient());
      //no verify the data (/packages.json) in ZK
      TestDistribPackageStore.assertResponseValues(1,
          () -> new MapWriterMap((Map) Utils.fromJSON(cluster.getZkClient().getData(SOLR_PKGS_PATH,
              null, new Stat(), true))),
          Utils.makeMap(
              ":packages:test_pkg[1]:version", "0.13",
              ":packages:test_pkg[1]:files[0]", FILE3
          ));

      //Now we will just delete one version
      Package.DelVersion delVersion = new Package.DelVersion();
      delVersion.version = "0.1";//this version does not exist
      delVersion.pkg = "test_pkg";
      req = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", delVersion))
          .build();

      //we are expecting an error
      expectError(req, cluster.getSolrClient(), errPath, "No such version:");

      delVersion.version = "0.12";//correct version. Should succeed
      req.process(cluster.getSolrClient());
      //Verify with ZK that the data is correcy
      TestDistribPackageStore.assertResponseValues(1,
          () -> new MapWriterMap((Map) Utils.fromJSON(cluster.getZkClient().getData(SOLR_PKGS_PATH,
              null, new Stat(), true))),
          Utils.makeMap(
              ":packages:test_pkg[0]:version", "0.13",
              ":packages:test_pkg[0]:files[0]", FILE2
          ));


      //So far we have been verifying the details with  ZK directly
      //use the package read API to verify with each node that it has the correct data
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        String path = jetty.getBaseUrl().toString().replace("/solr", "/api") + "/cluster/package?wt=javabin";
        TestDistribPackageStore.assertResponseValues(10, new Callable<NavigableObject>() {
          @Override
          public NavigableObject call() throws Exception {
            try (HttpSolrClient solrClient = (HttpSolrClient) jetty.newClient()) {
              return (NavigableObject) Utils.executeGET(solrClient.getHttpClient(), path, Utils.JAVABINCONSUMER);
            }
          }
        }, Utils.makeMap(
            ":result:packages:test_pkg[0]:version", "0.13",
            ":result:packages:test_pkg[0]:files[0]", FILE3
        ));
      }
    } finally {
      cluster.shutdown();
    }
  }

  static void postFileAndWait(MiniSolrCloudCluster cluster, String fname, String path, String sig) throws Exception {
    ByteBuffer fileContent = getFileContent(fname);
    String sha512 = DigestUtils.sha512Hex(fileContent.array());

    TestDistribPackageStore.postFile(cluster.getSolrClient(),
        fileContent,
        path, sig);// has file, but no signature

    TestDistribPackageStore.waitForAllNodesHaveFile(cluster, path, Utils.makeMap(
        ":files:" + path + ":sha512",
        sha512
    ), false);
  }

  private void expectError(V2Request req, SolrClient client, String errPath, String expectErrorMsg) throws IOException, SolrServerException {
    try {
      req.process(client);
      fail("should have failed with message : " + expectErrorMsg);
    } catch (BaseHttpSolrClient.RemoteExecutionException e) {
      String msg = e.getMetaData()._getStr(errPath, "");
      assertTrue("should have failed with message: " + expectErrorMsg + "actual message : " + msg,
          msg.contains(expectErrorMsg)
      );
    }
  }
}
