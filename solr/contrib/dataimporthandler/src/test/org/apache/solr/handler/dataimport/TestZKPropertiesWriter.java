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

import java.lang.invoke.MethodHandles;

import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestZKPropertiesWriter extends AbstractDataImportHandlerTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static ZkTestServer zkServer;

  protected static Path zkDir;

  private static CoreContainer cc;

  private String dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";

  @BeforeClass
  public static void dihZk_beforeClass() throws Exception {
    zkDir = createTempDir("zkData");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    System.setProperty("solrcloud.skip.autorecovery", "true");
    System.setProperty("zkHost", zkServer.getZkAddress());
    System.setProperty("jetty.port", "0000");

    zkServer.buildZooKeeper(getFile("dih/solr"),
        "dataimport-solrconfig.xml", "dataimport-schema.xml");

    //initCore("solrconfig.xml", "schema.xml", getFile("dih/solr").getAbsolutePath());
    cc = createDefaultCoreContainer(getFile("dih/solr").toPath());
  }

  @Before
  public void beforeDihZKTest() throws Exception {

  }

  @After
  public void afterDihZkTest() throws Exception {
    MockDataSource.clearCache();
  }


  @AfterClass
  public static void dihZk_afterClass() throws Exception {
    if (null != cc) {
      cc.shutdown();
      cc = null;
    }
    if (null != zkServer) {
      zkServer.shutdown();
      zkServer = null;
    }
    zkDir = null;
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to construct date stamps")
  @Test
  @SuppressWarnings({"unchecked"})
  public void testZKPropertiesWriter() throws Exception {
    // test using ZooKeeper
    assertTrue("Not using ZooKeeper", h.getCoreContainer().isZooKeeperAware());

    // for the really slow/busy computer, we wait to make sure we have a leader before starting
    h.getCoreContainer().getZkController().getZkStateReader().getLeaderUrl("collection1", "shard1", 30000);

    assertQ("test query on empty index", request("qlkciyopsbgzyvkylsjhchghjrdf"),
        "//result[@numFound='0']");

    SimpleDateFormat errMsgFormat = new SimpleDateFormat(dateFormat, Locale.ROOT);

    delQ("*:*");
    commit();
    SimpleDateFormat df = new SimpleDateFormat(dateFormat, Locale.ROOT);
    Date oneSecondAgo = new Date(System.currentTimeMillis() - 1000);

    Map<String, String> init = new HashMap<>();
    init.put("dateFormat", dateFormat);
    ZKPropertiesWriter spw = new ZKPropertiesWriter();
    spw.init(new DataImporter(h.getCore(), "dataimport"), init);
    Map<String, Object> props = new HashMap<>();
    props.put("SomeDates.last_index_time", oneSecondAgo);
    props.put("last_index_time", oneSecondAgo);
    spw.persist(props);

    @SuppressWarnings({"rawtypes"})
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "year_s", "2013"));
    MockDataSource.setIterator("select " + df.format(oneSecondAgo) + " from dummy", rows.iterator());

    h.query("/dataimport", lrf.makeRequest("command", "full-import", "dataConfig",
        generateConfig(), "clean", "true", "commit", "true", "synchronous",
        "true", "indent", "true"));
    props = spw.readIndexerProperties();
    Date entityDate = df.parse((String) props.get("SomeDates.last_index_time"));
    Date docDate = df.parse((String) props.get("last_index_time"));

    Assert.assertTrue("This date: " + errMsgFormat.format(oneSecondAgo) + " should be prior to the document date: " + errMsgFormat.format(docDate), docDate.getTime() - oneSecondAgo.getTime() > 0);
    Assert.assertTrue("This date: " + errMsgFormat.format(oneSecondAgo) + " should be prior to the entity date: " + errMsgFormat.format(entityDate), entityDate.getTime() - oneSecondAgo.getTime() > 0);
    assertQ(request("*:*"), "//*[@numFound='1']", "//doc/str[@name=\"year_s\"]=\"2013\"");

  }

  public SolrQueryRequest request(String... q) {
    LocalSolrQueryRequest req = lrf.makeRequest(q);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(req.getParams());
    params.set("distrib", true);
    req.setParams(params);
    return req;
  }

  protected String generateConfig() {
    StringBuilder sb = new StringBuilder();
    sb.append("<dataConfig> \n");
    sb.append("<propertyWriter dateFormat=\"" + dateFormat + "\" type=\"ZKPropertiesWriter\" />\n");
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
}
