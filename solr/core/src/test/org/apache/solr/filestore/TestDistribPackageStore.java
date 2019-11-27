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

package org.apache.solr.filestore;

import static org.apache.solr.common.util.Utils.JAVABINCONSUMER;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.server.ByteBufferInputStream;

import com.google.common.collect.ImmutableSet;

@LogLevel("org.apache.solr.filestore.PackageStoreAPI=DEBUG;org.apache.solr.filestore.DistribPackageStore=DEBUG")
public class TestDistribPackageStore extends SolrCloudTestCase {

  public void testPackageStoreManagement() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    try {

      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);

      try {
        postFile(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"),
            "/package/mypkg/v1.0/runtimelibs.jar",
            "j+Rflxi64tXdqosIhbusqi6GTwZq8znunC/dzwcWW0/dHlFGKDurOaE1Nz9FSPJuXbHkVLj638yZ0Lp1ssnoYA=="
        );
        fail("should have failed because of wrong signature ");
      } catch (RemoteExecutionException e) {
        assertTrue(e.getMessage().contains("Signature does not match"));
      }

      postFile(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"),
          "/package/mypkg/v1.0/runtimelibs.jar",
          "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ=="
          );

      assertResponseValues(10,
          cluster.getSolrClient(),
          new V2Request.Builder("/node/files/package/mypkg/v1.0")
              .withMethod(SolrRequest.METHOD.GET)
              .build(),
          Utils.makeMap(
              ":files:/package/mypkg/v1.0[0]:name", "runtimelibs.jar",
              ":files:/package/mypkg/v1.0[0]:sha512", "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420",
              ":files:/package/mypkg/v1.0[0]:sig[0]", "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ=="
          )
      );

      assertResponseValues(10,
          cluster.getSolrClient(),
          new V2Request.Builder("/node/files/package/mypkg")
              .withMethod(SolrRequest.METHOD.GET)
              .build(),
          Utils.makeMap(
              ":files:/package/mypkg[0]:name", "v1.0",
              ":files:/package/mypkg[0]:dir", "true"
          )
      );

      Map expected = Utils.makeMap(
          ":files:/package/mypkg/v1.0/runtimelibs.jar:name", "runtimelibs.jar",
          ":files:/package/mypkg/v1.0[0]:sha512", "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420"

      );
      waitForAllNodesHaveFile(cluster,"/package/mypkg/v1.0/runtimelibs.jar", expected, true);


      postFile(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
          "/package/mypkg/v1.0/runtimelibs_v2.jar",
          null
      );
      expected = Utils.makeMap(
          ":files:/package/mypkg/v1.0/runtimelibs_v2.jar:name", "runtimelibs_v2.jar",
          ":files:/package/mypkg/v1.0[0]:sha512",
          "bc5ce45ad281b6a08fb7e529b1eb475040076834816570902acb6ebdd809410e31006efdeaa7f78a6c35574f3504963f5f7e4d92247d0eb4db3fc9abdda5d417"

      );
      waitForAllNodesHaveFile(cluster,"/package/mypkg/v1.0/runtimelibs_v2.jar", expected, false);


      expected = Utils.makeMap(
          ":files:/package/mypkg/v1.0", (Predicate<Object>) o -> {
            List l = (List) o;
            assertEquals(2, l.size());
            Set expectedKeys = ImmutableSet.of("runtimelibs_v2.jar", "runtimelibs.jar");
            for (Object file : l) {
              if(! expectedKeys.contains(Utils.getObjectByPath(file, true, "name"))) return false;
            }

            return true;
          }
      );
      for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
        String baseUrl = jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api");
        String url = baseUrl + "/node/files/package/mypkg/v1.0?wt=javabin";

        assertResponseValues(10, new Fetcher(url, jettySolrRunner), expected);

      }



    } finally {
      cluster.shutdown();
    }
  }

  public static void waitForAllNodesHaveFile(MiniSolrCloudCluster cluster, String path, Map expected , boolean verifyContent) throws Exception {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      String baseUrl = jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api");
      String url = baseUrl + "/node/files" + path + "?wt=javabin&meta=true";
      assertResponseValues(10, new Fetcher(url, jettySolrRunner), expected);

      if(verifyContent) {
        try (HttpSolrClient solrClient = (HttpSolrClient) jettySolrRunner.newClient()) {
          ByteBuffer buf = Utils.executeGET(solrClient.getHttpClient(), baseUrl + "/node/files" + path,
              Utils.newBytesConsumer(Integer.MAX_VALUE));
          assertEquals(
              "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420",
              DigestUtils.sha512Hex(new ByteBufferInputStream(buf))
          );

        }
      }

    }
  }

  static class Fetcher implements Callable {
    String url;
    JettySolrRunner jetty;
    Fetcher(String s, JettySolrRunner jettySolrRunner){
      this.url = s;
      this.jetty = jettySolrRunner;
    }
    @Override
    public NavigableObject call() throws Exception {
      try (HttpSolrClient solrClient = (HttpSolrClient) jetty.newClient()) {
        return (NavigableObject) Utils.executeGET(solrClient.getHttpClient(), this.url, JAVABINCONSUMER);
      }
    }

    @Override
    public String toString() {
      return url;
    }

  }

  public static NavigableObject assertResponseValues(int repeats, SolrClient client, SolrRequest req, Map vals) throws Exception {
    Callable<NavigableObject> callable = () -> req.process(client);

    return assertResponseValues(repeats, callable,vals);
  }

  public static NavigableObject assertResponseValues(int repeats,  Callable<NavigableObject> callable,Map vals) throws Exception {
    NavigableObject rsp = null;

    for (int i = 0; i < repeats; i++) {
      if (i > 0) {
        Thread.sleep(100);
      }
      try {
        rsp = callable.call();
      } catch (Exception e) {
        if (i >= repeats - 1) throw e;
        continue;
      }
      for (Object e : vals.entrySet()) {
        Map.Entry entry = (Map.Entry) e;
        String k = (String) entry.getKey();
        List<String> key = StrUtils.split(k, '/');

        Object val = entry.getValue();
        Predicate p = val instanceof Predicate ? (Predicate) val : o -> {
          String v = o == null ? null : String.valueOf(o);
          return Objects.equals(val, o);
        };
        boolean isPass = p.test(rsp._get(key, null));
        if (isPass) return rsp;
        else if (i >= repeats - 1) {
          fail("req: " + callable.toString() +" . attempt: " + i + " Mismatch for value : '" + key + "' in response , " + Utils.toJSONString(rsp));
        }

      }

    }
    return rsp;
  }



  public static void postFile(SolrClient client, ByteBuffer buffer, String name, String sig)
      throws SolrServerException, IOException {
    String resource = "/cluster/files" + name;
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("sig", sig);
    V2Response rsp = new V2Request.Builder(resource)
        .withMethod(SolrRequest.METHOD.PUT)
        .withPayload(buffer)
        .forceV2(true)
        .withMimeType("application/octet-stream")
        .withParams(params)
        .build()
        .process(client);
    assertEquals(name, rsp.getResponse().get(CommonParams.FILE));
  }

  public static byte[] readFile(String fname) throws IOException {
    byte[] buf = null;
    try (FileInputStream fis = new FileInputStream(getFile(fname))) {
      buf = new byte[fis.available()];
      fis.read(buf);
    }
    return buf;
  }
}
