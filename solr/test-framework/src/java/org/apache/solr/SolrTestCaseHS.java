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
package org.apache.solr;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.servlet.DirectSolrConnection;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SolrTestCaseJ4.SuppressSSL
//@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Lucene45","Appending","Asserting"})
public class SolrTestCaseHS extends SolrTestCaseJ4 {
  
  public static final String SOLR_TESTS_SHARDS_WHITELIST = "solr.tests.shardsWhitelist";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @SafeVarargs
  public static <T> Set<T> set(T... a) {
    LinkedHashSet<T> s = new LinkedHashSet<>();
    for (T t : a) {
      s.add(t);
    }
    return s;
  }

  @SuppressWarnings({"unchecked"})
  public static <T> T rand(T... vals) {
    return vals[ random().nextInt(vals.length) ];
  }


  public static ModifiableSolrParams params(SolrParams params, String... moreParams) {
    ModifiableSolrParams msp = new ModifiableSolrParams(params);
    for (int i=0; i<moreParams.length; i+=2) {
      msp.add(moreParams[i], moreParams[i+1]);
    }
    return msp;
  }

  public static Map<String,Object> toObject(Doc doc, IndexSchema schema, Collection<String> fieldNames) {
    Map<String,Object> result = new HashMap<>();
    for (Fld fld : doc.fields) {
      if (fieldNames != null && !fieldNames.contains(fld.ftype.fname)) continue;
      SchemaField sf = schema.getField(fld.ftype.fname);
      if (!sf.multiValued()) {
        result.put(fld.ftype.fname, fld.vals.get(0));
      } else {
        result.put(fld.ftype.fname, fld.vals);
      }
    }
    return result;
  }
  
  
  @SuppressWarnings({"unchecked"})
  public static Object createDocObjects(@SuppressWarnings({"rawtypes"})Map<Comparable, Doc> fullModel,
                                        @SuppressWarnings({"rawtypes"})Comparator sort, int rows,
                                        Collection<String> fieldNames) {
    List<Doc> docList = new ArrayList<>(fullModel.values());
    Collections.sort(docList, sort);
    @SuppressWarnings({"rawtypes"})
    List sortedDocs = new ArrayList(rows);
    for (Doc doc : docList) {
      if (sortedDocs.size() >= rows) break;
      Map<String,Object> odoc = toObject(doc, h.getCore().getLatestSchema(), fieldNames);
      sortedDocs.add(toObject(doc, h.getCore().getLatestSchema(), fieldNames));
    }
    return sortedDocs;
  }


  public static void compare(SolrQueryRequest req, String path, Object model,
                             @SuppressWarnings({"rawtypes"})Map<Comparable, Doc> fullModel) throws Exception {
    String strResponse = h.query(req);

    Object realResponse = ObjectBuilder.fromJSON(strResponse);
    String err = JSONTestUtil.matchObj(path, realResponse, model);
    if (err != null) {
      log.error("RESPONSE MISMATCH: {}\n\trequest={}\n\tresult={}" +
          "\n\texpected={}\n\tmodel={}"
          , err, req, strResponse, JSONUtil.toJSON(model), fullModel);

      // re-execute the request... good for putting a breakpoint here for debugging
      String rsp = h.query(req);

      fail(err);
    }

  }

  /** Pass "null" for the client to query the local server */
  public static void assertJQ(SolrClient client, SolrParams args, String... tests) throws Exception {
    String resp;
    resp = getJSON(client, args);
    matchJSON(resp, tests);
  }

  public static void matchJSON(String response, String... tests) throws Exception {
    boolean failed = false;

    for (String test : tests) {
      if (test == null || test.length()==0) continue;

      try {
        failed = true;
        String err = JSONTestUtil.match(response, test, JSONTestUtil.DEFAULT_DELTA);
        failed = false;
        if (err != null) {
          log.error("query failed JSON validation. error={}\n expected ={}\n response = {}"
              , err, test, response
          );
          throw new RuntimeException(err);
        }
      } finally {
        if (failed) {
          log.error("JSON query validation threw an exception.\n expected ={}\n response = {}"
                  , test, response
          );
        }
      }
    }
  }

  /***
  public static void clearNCache() {
    SolrQueryRequest req = req();
    req.getSearcher().getnCache().clear();  // OFF-HEAP
    req.close();
  }***/

  public static void clearQueryCache() {
    SolrQueryRequest req = req();
    req.getSearcher();
    req.close();
  }


  public static String getQueryResponse(SolrClient client, String wt, SolrParams params) throws Exception {
    if (client == null) {
      return getQueryResponse(wt, params);
    }
    ModifiableSolrParams p = new ModifiableSolrParams(params);
    p.set("wt", wt);
    String path = p.get("qt");
    p.remove("qt");
    p.set("indent","true");

    QueryRequest query = new QueryRequest( p );
    if (path != null) {
      query.setPath(path);
    }

    if ("json".equals(wt)) {
      query.setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
      NamedList<Object> rsp = client.request(query);
      return Utils.toJSONString(rsp);
    } else {
      query.setResponseParser(new NoOpResponseParser(wt));
      NamedList<Object> rsp = client.request(query);
      return  (String)rsp.get("response");
    }
  }

  public static String getQueryResponse(String wt, SolrParams params) throws Exception {
    ModifiableSolrParams p = new ModifiableSolrParams(params);
    p.set("wt", wt);
    String path = p.get("qt");
    p.remove("qt");
    p.set("indent","true");

    DirectSolrConnection connection = new DirectSolrConnection(h.getCore());
    String raw = connection.request(path, p, null);
    return raw;
  }


  public static String getJSON(SolrClient client, SolrParams params) throws Exception {
    return getQueryResponse(client, "json", params);
  }

  /** Adds a document using the specific client, or to the local test core if null.
   * Returns the version.  TODO: work in progress... version not always returned.  */
  public static Long add(SolrClient client, SolrInputDocument sdoc, ModifiableSolrParams params) throws Exception {
    if (client == null) {
      Long version = addAndGetVersion( sdoc, params );
      return version;
    } else {
      UpdateRequest updateRequest = new UpdateRequest();
      if (params != null) {
        updateRequest.setParams(params);
      }
      updateRequest.add( sdoc );
      UpdateResponse rsp = updateRequest.process( client );
      // TODO - return version
      return null;
    }
  }



  public static class Client {
    ClientProvider provider;
    ModifiableSolrParams queryDefaults;
    public Tester tester = new Tester();

    public static class Tester {
      public  void assertJQ(SolrClient client, SolrParams args, String... tests) throws Exception {
        SolrTestCaseHS.assertJQ(client, args, tests);
      }
    }

    public static Client localClient = new Client(null, 1);
    public static Client localClient() {
      return new Client(null, 1);
    }

    public Client(List<SolrClient> clients, int seed) {
      if (clients != null) {
        provider = new ClientProvider(clients, seed);
      }
    }

    public static int hash(int x) {
      // from Thomas Mueller
      x = ((x >>> 16) ^ x) * 0x45d9f3b;
      x = ((x >>> 16) ^ x) * 0x45d9f3b;
      x = ((x >>> 16) ^ x);
      return x;
    }

    public ModifiableSolrParams queryDefaults() {
      if (queryDefaults == null) {
        queryDefaults = new ModifiableSolrParams();
      }
      return queryDefaults;
    }

    public boolean local() {
      return provider == null;
    }
    public ClientProvider getClientProvider() { return provider; }

    public void testJQ(SolrParams args, String... tests) throws Exception {
      if (queryDefaults != null) {
        ModifiableSolrParams newParams = params(queryDefaults);
        newParams.add(args);
        args = newParams;
      }
      SolrClient client = provider==null ? null : provider.client(null, args);
      tester.assertJQ(client, args, tests);
    }

    public Long add(SolrInputDocument sdoc, ModifiableSolrParams params) throws Exception {
      SolrClient client = provider==null ? null : provider.client(sdoc, params);
      return SolrTestCaseHS.add(client, sdoc, params);
    }

    public void commit() throws IOException, SolrServerException {
      if (local()) {
        assertU(SolrTestCaseJ4.commit());
        return;
      }

      for (SolrClient client : provider.all()) {
        client.commit();
      }
    }

    public void deleteByQuery(String query, ModifiableSolrParams params) throws IOException, SolrServerException {
      if (local()) {
        assertU(delQ(query));  // todo - handle extra params
        return;
      }

      for (SolrClient client : provider.all()) {
        client.deleteByQuery(query); // todo - handle extra params
      }
    }

  }


  public static class ClientProvider {
    public static String idField = "id";

    List<SolrClient> clients;
    Random r;
    int hashSeed;

    // thisIsIgnored needed because we need a diff signature
    public ClientProvider(List<SolrClient> clients, int seed) {
      this.hashSeed = Client.hash(seed);
      this.clients = clients;
      r = new Random(seed);
    }

    public SolrClient client(SolrInputDocument sdoc, SolrParams params) {
      String idStr = null;
      if (sdoc != null) {
        idStr = sdoc.getFieldValue(idField).toString();
      } else if (params!=null) {
        idStr = params.get(idField);
      }

      int hash;
      if (idStr != null) {
        // make the client chosen the same for a duplicate ID
        hash = idStr.hashCode() ^ hashSeed;
      } else {
        hash = r.nextInt();
      }


      return clients.get( (hash & Integer.MAX_VALUE) % clients.size() );
    }

    public List<SolrClient> all() {
      return clients;
    }

    public int getSeed() {
      return hashSeed;
    }
  }


  //
  // Helper to run an internal Jetty instance.
  // Example:
  //  SolrInstance s1 = new SolrInstance(createTempDir("s1"), "solrconfig-tlog.xml", "schema_latest.xml");
  //  s1.start();
  //  SolrClient c1 = s1.getSolrJ();
  //  assertJQ(c1, params("q", "*:*"), "/response/numFound==3");
  //  String json = getJSON(c1, params("q","id:1"));
  //  s1.stop();
  //
  // To manage multiple servers, see SolrInstances
  //
  public static class SolrInstance {
    private static Logger log = SolrTestCaseHS.log;
    private String collection = "collection1";
    private int port = 0;
    private String solrconfigFile;
    private String schemaFile;
    private File baseDir;
    private JettySolrRunner jetty;
    private SolrClient solrj;

    private boolean homeCreated = false;


    public SolrInstance(File homeDir, String solrconfigFile, String schemaFile) {
      this.baseDir = homeDir;
      this.solrconfigFile = solrconfigFile;
      this.schemaFile = schemaFile;
    }

    public String getBaseDir() {
      return baseDir.toString();
    }

    public String getBaseURL() {
      return (SolrTestCaseJ4.isSSLMode() ? "https" : "http") + "://127.0.0.1:" + port + "/solr";
    }

    public String getCollectionURL() {
      return getBaseURL() + "/" + collection;
    }

    /** string appropriate for passing in shards param (i.e. missing http://) */
    public String getShardURL() {
      return "127.0.0.1:" + port + "/solr" + "/" + collection;
    }

    public SolrClient getSolrJ() {
      if (solrj == null) {
        solrj = getHttpSolrClient(getCollectionURL());
      }
      return solrj;
    }

    /** If it needs to change */
    public void setPort(int port) {
      this.port = port;
    }

    public void createHome() throws Exception {
      homeCreated=true;
      SolrTestCaseJ4.copySolrHomeToTemp(baseDir, collection);
      copyConfFile(baseDir, collection, solrconfigFile);
      copyConfFile(baseDir, collection, schemaFile);

      File collDir = new File(baseDir, collection);
      try (Writer w = new OutputStreamWriter(Files.newOutputStream(collDir.toPath().resolve("core.properties")), StandardCharsets.UTF_8)) {
        Properties coreProps = new Properties();
        coreProps.put("name", "collection1");
        coreProps.put("config", solrconfigFile);
        coreProps.put("schema", schemaFile);
        coreProps.store(w, "");
      }
    }


    public void start() throws Exception {
      if (!homeCreated) {
        createHome();
      }

      if (jetty == null) {
        JettyConfig jettyConfig = JettyConfig.builder()
            .stopAtShutdown(true)
            .setContext("/solr")
            .setPort(port)
            .withSSLConfig(sslConfig.buildServerSSLConfig())
            .build();
        Properties nodeProperties = new Properties();
        nodeProperties.setProperty("solrconfig", solrconfigFile);
        nodeProperties.setProperty(CoreDescriptor.CORE_SCHEMA, schemaFile);
        jetty = new JettySolrRunner(baseDir.getAbsolutePath(), nodeProperties, jettyConfig);
      }

      // silly stuff included from solrconfig.snippet.randomindexconfig.xml
      System.setProperty("solr.tests.maxBufferedDocs", String.valueOf(100000));
      
      // If we want to run with whitelist list, this must be explicitly set to true for the test
      // otherwise we disable the check
      if (System.getProperty(SYSTEM_PROPERTY_SOLR_DISABLE_SHARDS_WHITELIST) == null) {
        systemSetPropertySolrDisableShardsWhitelist("true");
      }

      jetty.start();
      port = jetty.getLocalPort();
      if (log.isInfoEnabled()) {
        log.info("===> Started solr server port={} home={}", port, getBaseDir());
      }
    }

    public void stop() throws Exception {
      jetty.stop();
      if (solrj != null) solrj.close();
    }

    public void tearDown() throws Exception {
      IOUtils.deleteFilesIfExist(baseDir.toPath());
    }

    private static void copyConfFile(File dstRoot, String destCollection, String file) throws Exception {
      File subHome = new File(dstRoot, destCollection + File.separator + "conf");
      String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
      FileUtils.copyFile(new File(top, file), new File(subHome, file));
    }

    public void copyConfigFile(File dstRoot, String destCollection, String file) throws Exception {
      if (!homeCreated) {
        createHome();
      }

      File subHome = new File(dstRoot, destCollection + File.separator + "conf");
      String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
      FileUtils.copyFile(new File(top, file), new File(subHome, file));
    }

  }


  //  Manages a number of Solr servers and provides a Client to partition documents and randomly assign query requests.
  //  Example:
  //        SolrInstances servers = new SolrInstances(3, "solrconfig-tlog.xml","schema_latest.xml");
  //        Client = servers.getClient(0);
  //        client.add(sdoc("id", "3"), null);
  //        client.commit();
  //        client.testJQ(params("q", "*:*"), "/response/numFound==3")
  //        servers.stop();
  //
  public static class SolrInstances {
    public List<SolrInstance> slist;
    public Client client;

    public SolrInstances(int numServers, String solrconfig, String schema) throws Exception {
      slist = new ArrayList<>(numServers);
      for (int i=0; i<numServers; i++) {
        SolrInstance instance = new SolrInstance(createTempDir("s"+ i).toFile(), solrconfig, schema);
        slist.add(instance);
        instance.start();
      }
    }

    public void stop() throws Exception {
      for (SolrInstance instance : slist) {
        instance.stop();
      }
    }

    // For params.set("shards", getShards())
    public String getShards() {
      return getShardsParam(slist);
    }
    
    public String getWhitelistString() {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (SolrInstance instance : slist) {
        if (first) {
          first = false;
        } else {
          sb.append(',');
        }
        sb.append( instance.getBaseURL().replace("/solr", ""));
      }
      return sb.toString();
    }

    public List<SolrClient> getSolrJs() {
      List<SolrClient> solrjs = new ArrayList<>(slist.size());
      for (SolrInstance instance : slist) {
        solrjs.add( instance.getSolrJ() );
      }
      return solrjs;
    }

    public Client getClient(int seed) {
      if (client == null) {
        client = new Client(getSolrJs(), seed);
      }
      return client;
    }

    public static String getShardsParam(List<SolrInstance> instances) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (SolrInstance instance : instances) {
        if (first) {
          first = false;
        } else {
          sb.append(',');
        }
        sb.append( instance.getShardURL() );
      }
      return sb.toString();
    }



  }

}
