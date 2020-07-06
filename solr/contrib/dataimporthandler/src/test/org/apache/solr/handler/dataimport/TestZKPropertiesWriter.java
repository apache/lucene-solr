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
package org.apache.solr.handler.dataimport;

import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.BaseTestHarness;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that DIH properties writer works when using Zookeeper. Zookeeper is used by virtue of starting a SolrCloud cluster.<p>
 *
 * Note this test is an unelegant bridge between code that assumes a non SolrCloud environment and that would normally use
 * test infra that is not meant to work in a SolrCloud environment ({@link org.apache.solr.util.TestHarness} and some methods in
 * {@link org.apache.solr.SolrTestCaseJ4}) and between a test running SolrCloud (extending {@link SolrCloudTestCase} and
 * using {@link MiniSolrCloudCluster}).<p>
 *
 * These changes were introduced when https://issues.apache.org/jira/browse/SOLR-12823 got fixed and the legacy
 * behaviour of SolrCloud that allowed a SolrCloud (Zookeeper active) to function like a standalone Solr (in which the
 * cluster would adopt cores contributed by the nodes even if they were unknown to Zookeeper) was no more.
 */
public class TestZKPropertiesWriter extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static ZkTestServer zkServer;

  private static MiniSolrCloudCluster minicluster;

  private String dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";

  @BeforeClass
  public static void dihZk_beforeClass() throws Exception {
    System.setProperty(DataImportHandler.ENABLE_DIH_DATA_CONFIG_PARAM, "true");

    minicluster = configureCluster(1)
        .addConfig("conf", configset("dihconfigset"))
        .configure();

    zkServer = minicluster.getZkServer();
  }

  @After
  public void afterDihZkTest() throws Exception {
    MockDataSource.clearCache();
  }

  @AfterClass
  public static void dihZk_afterClass() throws Exception {
    shutdownCluster();
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to construct date stamps")
  @Test
  @SuppressWarnings({"unchecked"})
  public void testZKPropertiesWriter() throws Exception {
    CollectionAdminRequest.createCollectionWithImplicitRouter("collection1", "conf", "1", 1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());

    // DIH talks core, SolrCloud talks collection.
    DocCollection coll = getCollectionState("collection1");
    Replica replica = coll.getReplicas().iterator().next();
    JettySolrRunner jetty = minicluster.getReplicaJetty(replica);
    SolrCore core = jetty.getCoreContainer().getCore(replica.getCoreName());

    localAssertQ("test query on empty index", request(core, "qlkciyopsbgzyvkylsjhchghjrdf"), "//result[@numFound='0']");

    SimpleDateFormat errMsgFormat = new SimpleDateFormat(dateFormat, Locale.ROOT);

    // These two calls are from SolrTestCaseJ4 and end up in TestHarness... That's ok they are static and do not reference
    // the various variables that were not initialized (so not copying them to this test class as some other methods at the bottom).
    delQ("*:*");
    commit();
    SimpleDateFormat df = new SimpleDateFormat(dateFormat, Locale.ROOT);
    Date oneSecondAgo = new Date(System.currentTimeMillis() - 1000);

    Map<String, String> init = new HashMap<>();
    init.put("dateFormat", dateFormat);
    ZKPropertiesWriter spw = new ZKPropertiesWriter();
    spw.init(new DataImporter(core, "dataimport"), init);
    Map<String, Object> props = new HashMap<>();
    props.put("SomeDates.last_index_time", oneSecondAgo);
    props.put("last_index_time", oneSecondAgo);
    spw.persist(props);

    @SuppressWarnings({"rawtypes"})
    List rows = new ArrayList();
    rows.add(AbstractDataImportHandlerTestCase.createMap("id", "1", "year_s", "2013"));
    MockDataSource.setIterator("select " + df.format(oneSecondAgo) + " from dummy", rows.iterator());

    localQuery("/dataimport", localMakeRequest(core, "command", "full-import", "dataConfig",
        generateConfig(), "clean", "true", "commit", "true", "synchronous",
        "true", "indent", "true"));
    props = spw.readIndexerProperties();
    Date entityDate = df.parse((String) props.get("SomeDates.last_index_time"));
    Date docDate = df.parse((String) props.get("last_index_time"));

    Assert.assertTrue("This date: " + errMsgFormat.format(oneSecondAgo) + " should be prior to the document date: " + errMsgFormat.format(docDate), docDate.getTime() - oneSecondAgo.getTime() > 0);
    Assert.assertTrue("This date: " + errMsgFormat.format(oneSecondAgo) + " should be prior to the entity date: " + errMsgFormat.format(entityDate), entityDate.getTime() - oneSecondAgo.getTime() > 0);
    localAssertQ("Should have found 1 doc, year 2013", request(core, "*:*"), "//*[@numFound='1']", "//doc/str[@name=\"year_s\"]=\"2013\"");

    core.close();
  }

  private static SolrQueryRequest request(SolrCore core, String... q) {
    LocalSolrQueryRequest req = localMakeRequest(core, q);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(req.getParams());
    params.set("distrib", true);
    req.setParams(params);
    return req;
  }

  private String generateConfig() {
    StringBuilder sb = new StringBuilder();
    sb.append("<dataConfig> \n");
    sb.append("<propertyWriter dateFormat=\"").append(dateFormat).append("\" type=\"ZKPropertiesWriter\" />\n");
    sb.append("<dataSource name=\"mock\" type=\"MockDataSource\"/>\n");
    sb.append("<document name=\"TestSimplePropertiesWriter\"> \n");
    sb.append("<entity name=\"SomeDates\" processor=\"SqlEntityProcessor\" dataSource=\"mock\" ");
    sb.append("query=\"select ${dih.last_index_time} from dummy\" >\n");
    sb.append("<field column=\"AYEAR_S\" name=\"year_s\" /> \n");
    sb.append("</entity>\n");
    sb.append("</document> \n");
    sb.append("</dataConfig> \n");
    String config = sb.toString();
    log.debug(config);
    return config;
  }

  /**
   * Code copied with some adaptations from {@link org.apache.solr.util.TestHarness.LocalRequestFactory#makeRequest(String...)}.
   */
  @SuppressWarnings({"unchecked"})
  private static LocalSolrQueryRequest localMakeRequest(SolrCore core, String ... q) {
    if (q.length==1) {
      Map<String, String> args = new HashMap<>();
      args.put(CommonParams.VERSION,"2.2");

      return new LocalSolrQueryRequest(core, q[0], "", 0, 20, args);
    }
    if (q.length%2 != 0) {
      throw new RuntimeException("The length of the string array (query arguments) needs to be even");
    }
    @SuppressWarnings({"rawtypes"})
    Map.Entry<String, String> [] entries = new NamedList.NamedListEntry[q.length / 2];
    for (int i = 0; i < q.length; i += 2) {
      entries[i/2] = new NamedList.NamedListEntry<>(q[i], q[i+1]);
    }
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList(entries);
    if(nl.get("wt" ) == null) nl.add("wt","xml");
    return new LocalSolrQueryRequest(core, nl);
  }

  /**
   * Code copied from {@link org.apache.solr.util.TestHarness#query(String, SolrQueryRequest)} because it is not
   * <code>static</code> there (it could have been) and we do not have an instance of {@link org.apache.solr.util.TestHarness}.
   */
  private static String localQuery(String handler, SolrQueryRequest req) throws Exception {
    try {
      SolrCore core = req.getCore();
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      core.execute(core.getRequestHandler(handler),req,rsp); // TODO the core doesn't have the request handler
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
      if (responseWriter instanceof BinaryQueryResponseWriter) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(32000);
        BinaryQueryResponseWriter writer = (BinaryQueryResponseWriter) responseWriter;
        writer.write(byteArrayOutputStream, req, rsp);
        return new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
      } else {
        StringWriter sw = new StringWriter(32000);
        responseWriter.write(sw,req,rsp);
        return sw.toString();
      }

    } finally {
      req.close();
      SolrRequestInfo.clearRequestInfo();
    }
  }

  /**
   * Code copied from {@link org.apache.solr.SolrTestCaseJ4#assertQ(String, SolrQueryRequest, String...)} in order not to
   * use the instance of the {@link org.apache.solr.util.TestHarness}.
   */
  private static void localAssertQ(String message, SolrQueryRequest req, String... tests) {
    try {
      String m = (null == message) ? "" : message + " "; // TODO log 'm' !!!
      //since the default (standard) response format is now JSON
      //need to explicitly request XML since this class uses XPath
      ModifiableSolrParams xmlWriterTypeParams = new ModifiableSolrParams(req.getParams());
      xmlWriterTypeParams.set(CommonParams.WT,"xml");
      //for tests, let's turn indention off so we don't have to handle extraneous spaces
      xmlWriterTypeParams.set("indent", xmlWriterTypeParams.get("indent", "off"));
      req.setParams(xmlWriterTypeParams);
      String response = localQuery(req.getParams().get(CommonParams.QT), req);

      if (req.getParams().getBool("facet", false)) {
        // add a test to ensure that faceting did not throw an exception
        // internally, where it would be added to facet_counts/exception
        String[] allTests = new String[tests.length+1];
        System.arraycopy(tests,0,allTests,1,tests.length);
        allTests[0] = "*[count(//lst[@name='facet_counts']/*[@name='exception'])=0]";
        tests = allTests;
      }

      String results = BaseTestHarness.validateXPath(response, tests);

      if (null != results) {
        String msg = "REQUEST FAILED: xpath=" + results
            + "\n\txml response was: " + response
            + "\n\trequest was:" + req.getParamString();

        log.error(msg);
        throw new RuntimeException(msg);
      }

    } catch (XPathExpressionException e1) {
      throw new RuntimeException("XPath is invalid", e1);
    } catch (Exception e2) {
      SolrException.log(log,"REQUEST FAILED: " + req.getParamString(), e2);
      throw new RuntimeException("Exception during query", e2);
    }
  }
}
