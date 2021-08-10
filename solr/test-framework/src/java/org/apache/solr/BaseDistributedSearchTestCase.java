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
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.Filter;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

/**
 * Helper base class for distributed search test cases
 *
 * By default, all tests in sub-classes will be executed with
 * 1, 2, ... DEFAULT_MAX_SHARD_COUNT number of shards set up repeatedly.
 *
 * In general, it's preferable to annotate the tests in sub-classes with a
 * {@literal @}ShardsFixed(num = N) or a {@literal @}ShardsRepeat(min = M, max = N)
 * to indicate whether the test should be called once, with a fixed number of shards,
 * or called repeatedly for number of shards = M to N.
 *
 * In some cases though, if the number of shards has to be fixed, but the number
 * itself is dynamic, or if it has to be set as a default for all sub-classes
 * of a sub-class, there's a fixShardCount(N) available, which is identical to
 * {@literal @}ShardsFixed(num = N) for all tests without annotations in that class
 * hierarchy. Ideally this function should be retired in favour of better annotations..
 *
 * @since solr 1.5
 */
public abstract class BaseDistributedSearchTestCase extends SolrTestCaseJ4 {
  
  protected ExecutorService executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
      4,
      Integer.MAX_VALUE,
      15, TimeUnit.SECONDS, // terminate idle threads after 15 sec
      new SynchronousQueue<>(),  // directly hand off tasks
      new SolrNamedThreadFactory("BaseDistributedSearchTestCase"),
      false
  );
  
  // TODO: this shouldn't be static. get the random when you need it to avoid sharing.
  public static Random r;
  
  private AtomicInteger nodeCnt = new AtomicInteger(0);
  protected boolean useExplicitNodeNames;
  
  @BeforeClass
  public static void initialize() {
    assumeFalse("SOLR-4147: ibm 64bit has jvm bugs!", Constants.JRE_IS_64BIT && Constants.JAVA_VENDOR.startsWith("IBM"));
    r = new Random(random().nextLong());
  }
  
  /**
   * Set's the value of the "hostContext" system property to a random path 
   * like string (which may or may not contain sub-paths).  This is used 
   * in the default constructor for this test to help ensure no code paths have
   * hardcoded assumptions about the servlet context used to run solr.
   * <p>
   * Test configs may use the <code>${hostContext}</code> variable to access 
   * this system property.
   * </p>
   * @see #BaseDistributedSearchTestCase()
   * @see #clearHostContext
   */
  @BeforeClass
  public static void initHostContext() {
    // Can't use randomRealisticUnicodeString because unescaped unicode is 
    // not allowed in URL paths
    // Can't use URLEncoder.encode(randomRealisticUnicodeString) because
    // Jetty freaks out and returns 404's when the context uses escapes

    StringBuilder hostContext = new StringBuilder("/");
    if (random().nextBoolean()) {
      // half the time we use the root context, the other half...

      // Remember: randomSimpleString might be the empty string
      hostContext.append(TestUtil.randomSimpleString(random(), 2));
      if (random().nextBoolean()) {
        hostContext.append("_");
      }
      hostContext.append(TestUtil.randomSimpleString(random(), 3));
      if ( ! "/".equals(hostContext.toString())) {
        // if our random string is empty, this might add a trailing slash, 
        // but our code should be ok with that
        hostContext.append("/").append(TestUtil.randomSimpleString(random(), 2));
      } else {
        // we got 'lucky' and still just have the root context,
        // NOOP: don't try to add a subdir to nothing (ie "//" is bad)
      }
    }
    // paranoia, we *really* don't want to ever get "//" in a path...
    final String hc = hostContext.toString().replaceAll("\\/+","/");

    log.info("Setting hostContext system property: {}", hc);
    System.setProperty("hostContext", hc);
  }
  
  /**
   * Clears the "hostContext" system property
   * @see #initHostContext
   */
  @AfterClass
  public static void clearHostContext() throws Exception {
    System.clearProperty("hostContext");
  }

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void setSolrDisableShardsWhitelist() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void clearSolrDisableShardsWhitelist() throws Exception {
    systemClearPropertySolrDisableShardsWhitelist();
  }

  private static String getHostContextSuitableForServletContext() {
    String ctx = System.getProperty("hostContext","/solr");
    if ("".equals(ctx)) ctx = "/solr";
    if (ctx.endsWith("/")) ctx = ctx.substring(0,ctx.length()-1);;
    if (!ctx.startsWith("/")) ctx = "/" + ctx;
    return ctx;
  }

  /**
   * Constructs a test in which the jetty+solr instances as well as the 
   * solr clients all use the value of the "hostContext" system property.
   * <p>
   * If the system property is not set, or is set to the empty string 
   * (neither of which should normally happen unless a subclass explicitly 
   * modifies the property set by {@link #initHostContext} prior to calling 
   * this constructor) a servlet context of "/solr" is used. (this is for 
   * consistency with the default behavior of solr.xml parsing when using 
   * <code>hostContext="${hostContext:}"</code>
   * </p>
   * <p>
   * If the system property is set to a value which does not begin with a 
   * "/" (which should normally happen unless a subclass explicitly 
   * modifies the property set by {@link #initHostContext} prior to calling 
   * this constructor) a leading "/" will be prepended.
   * </p>
   *
   * @see #initHostContext
   */
  protected BaseDistributedSearchTestCase() {
    this(getHostContextSuitableForServletContext());
  }

  /**
   * @param context explicit servlet context path to use (eg: "/solr")
   */
  protected BaseDistributedSearchTestCase(final String context) {
    this.context = context;
    this.deadServers = new String[] {DEAD_HOST_1 + context,
                                     DEAD_HOST_2 + context,
                                     DEAD_HOST_3 + context};
  }

  private final static int DEFAULT_MAX_SHARD_COUNT = 3;

  private int shardCount = -1;      // the actual number of solr cores that will be created in the cluster
  public int getShardCount() {
    return shardCount;
  }

  private boolean isShardCountFixed = false;

  public void fixShardCount(int count) {
    isShardCountFixed = true;
    shardCount = count;
  }

  protected volatile JettySolrRunner controlJetty;
  protected final List<SolrClient> clients = Collections.synchronizedList(new ArrayList<>());
  protected final List<JettySolrRunner> jettys = Collections.synchronizedList(new ArrayList<>());
  
  protected volatile String context;
  protected volatile String[] deadServers;
  protected volatile String shards;
  protected volatile String[] shardsArr;
  protected volatile File testDir;
  protected volatile SolrClient controlClient;

  // to stress with higher thread counts and requests, make sure the junit
  // xml formatter is not being used (all output will be buffered before
  // transformation to xml and cause an OOM exception).
  protected volatile int stress = TEST_NIGHTLY ? 2 : 0;
  protected volatile boolean verifyStress = true;
  protected volatile int nThreads = 3;

  public final static int ORDERED = 1;
  public final static int SKIP = 2;
  public final static int SKIPVAL = 4;
  public final static int UNORDERED = 8;
  
  /**
   * When this flag is set, Double values will be allowed a difference ratio of 1E-8
   * between the non-distributed and the distributed returned values
   */
  public static int FUZZY = 16;
  private static final double DOUBLE_RATIO_LIMIT = 1E-8;

  protected volatile int flags;
  protected Map<String, Integer> handle = new ConcurrentHashMap<>();

  protected String id = "id";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static RandVal rint = new RandVal() {
    @Override
    public Object val() {
      return r.nextInt();
    }
  };

  public static RandVal rlong = new RandVal() {
    @Override
    public Object val() {
      return r.nextLong();
    }
  };

  public static RandVal rfloat = new RandVal() {
    @Override
    public Object val() {
      return r.nextFloat();
    }
  };

  public static RandVal rdouble = new RandVal() {
    @Override
    public Object val() {
      return r.nextDouble();
    }
  };

  public static RandVal rdate = new RandDate();

  public static String[] fieldNames = new String[]{"n_ti1", "n_f1", "n_tf1", "n_d1", "n_td1", "n_l1", "n_tl1", "n_dt1", "n_tdt1"};
  public static RandVal[] randVals = new RandVal[]{rint, rfloat, rfloat, rdouble, rdouble, rlong, rlong, rdate, rdate};

  protected String[] getFieldNames() {
    return fieldNames;
  }

  protected RandVal[] getRandValues() {
    return randVals;
  }

  /**
   * Subclasses can override this to change a test's solr home
   * (default is in test-files)
   */
  public String getSolrHome() {
    return SolrTestCaseJ4.TEST_HOME();
  }

  private boolean distribSetUpCalled = false;
  public void distribSetUp() throws Exception {
    distribSetUpCalled = true;
    SolrTestCaseJ4.resetExceptionIgnores();  // ignore anything with ignore_exception in it
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    testDir = createTempDir().toFile();
  }

  private volatile boolean distribTearDownCalled = false;
  public void distribTearDown() throws Exception {
    ExecutorUtil.shutdownAndAwaitTermination(executor);
    distribTearDownCalled = true;
  }

  protected JettySolrRunner createControlJetty() throws Exception {
    Path jettyHome = testDir.toPath().resolve("control");
    File jettyHomeFile = jettyHome.toFile();
    seedSolrHome(jettyHomeFile);
    seedCoreRootDirWithDefaultTestCore(jettyHome.resolve("cores"));
    JettySolrRunner jetty = createJetty(jettyHomeFile, null, null, getSolrConfigFile(), getSchemaFile());
    jetty.start();
    return jetty;
  }

  protected void createServers(int numShards) throws Exception {

    System.setProperty("configSetBaseDir", getSolrHome());

    controlJetty = createControlJetty();
    controlClient = createNewSolrClient(controlJetty.getLocalPort());

    shardsArr = new String[numShards];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      final String shardname = "shard" + i;
      Path jettyHome = testDir.toPath().resolve(shardname);
      File jettyHomeFile = jettyHome.toFile();
      seedSolrHome(jettyHomeFile);
      seedCoreRootDirWithDefaultTestCore(jettyHome.resolve("cores"));
      JettySolrRunner j = createJetty(jettyHomeFile, null, null, getSolrConfigFile(), getSchemaFile());
      j.start();
      jettys.add(j);
      clients.add(createNewSolrClient(j.getLocalPort()));
      String shardStr = buildUrl(j.getLocalPort());

      if (shardStr.endsWith("/")) shardStr += DEFAULT_TEST_CORENAME;
      else shardStr += "/" + DEFAULT_TEST_CORENAME;

      shardsArr[i] = shardStr;
      sb.append(shardStr);
    }

    shards = sb.toString();
  }


  protected void setDistributedParams(ModifiableSolrParams params) {
    params.set("shards", getShardsString());
  }

  protected String getShardsString() {
    if (deadServers == null) return shards;
    
    StringBuilder sb = new StringBuilder();
    for (String shard : shardsArr) {
      if (sb.length() > 0) sb.append(',');
      int nDeadServers = r.nextInt(deadServers.length+1);
      if (nDeadServers > 0) {
        List<String> replicas = new ArrayList<>(Arrays.asList(deadServers));
        Collections.shuffle(replicas, r);
        replicas.add(r.nextInt(nDeadServers+1), shard);
        for (int i=0; i<nDeadServers+1; i++) {
          if (i!=0) sb.append('|');
          sb.append(replicas.get(i));
        }
      } else {
        sb.append(shard);
      }
    }

    return sb.toString();
  }

  protected void destroyServers() throws Exception {
    ExecutorService customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("closeThreadPool"));
    
    customThreadPool.submit(() -> IOUtils.closeQuietly(controlClient));

    customThreadPool.submit(() -> {
      try {
        controlJetty.stop();
      } catch (NullPointerException e) {
        // ignore
      } catch (Exception e) {
        log.error("Error stopping Control Jetty", e);
      }
    });

    for (SolrClient client : clients) {
      customThreadPool.submit(() ->  IOUtils.closeQuietly(client));
    }
    
    for (JettySolrRunner jetty : jettys) {
      customThreadPool.submit(() -> {
        try {
          jetty.stop();
        } catch (Exception e) {
          log.error("Error stopping Jetty", e);
        }
      });
    }

    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
    
    clients.clear();
    jettys.clear();
  }
  
  public JettySolrRunner createJetty(File solrHome, String dataDir) throws Exception {
    return createJetty(solrHome, dataDir, null, null, null);
  }

  public JettySolrRunner createJetty(File solrHome, String dataDir, String shardId) throws Exception {
    return createJetty(solrHome, dataDir, shardId, null, null);
  }
  
  public JettySolrRunner createJetty(File solrHome, String dataDir, String shardList, String solrConfigOverride, String schemaOverride) throws Exception {
    return createJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, useExplicitNodeNames);
  }
  
  public JettySolrRunner createJetty(File solrHome, String dataDir, String shardList, String solrConfigOverride, String schemaOverride, boolean explicitCoreNodeName) throws Exception {

    Properties props = new Properties();
    if (solrConfigOverride != null)
      props.setProperty("solrconfig", solrConfigOverride);
    if (schemaOverride != null)
      props.setProperty("schema", schemaOverride);
    if (shardList != null)
      props.setProperty("shards", shardList);
    if (dataDir != null) {
      props.setProperty("solr.data.dir", dataDir);
    }
    if (explicitCoreNodeName) {
      props.setProperty("coreNodeName", Integer.toString(nodeCnt.incrementAndGet()));
    }
    props.setProperty("coreRootDirectory", solrHome.toPath().resolve("cores").toAbsolutePath().toString());

    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(), props, JettyConfig.builder()
        .stopAtShutdown(true)
        .setContext(context)
        .withFilters(getExtraRequestFilters())
        .withServlets(getExtraServlets())
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build());

    return jetty;
  }
  
  /** Override this method to insert extra servlets into the JettySolrRunners that are created using createJetty() */
  public SortedMap<ServletHolder,String> getExtraServlets() {
    return null;
  }

  /** Override this method to insert extra filters into the JettySolrRunners that are created using createJetty() */
  public SortedMap<Class<? extends Filter>,String> getExtraRequestFilters() {
    return null;
  }

  protected SolrClient createNewSolrClient(int port) {
    try {
      // setup the client...
      String baseUrl = buildUrl(port);
      if (baseUrl.endsWith("/")) {
        return getHttpSolrClient(baseUrl + DEFAULT_TEST_CORENAME);
      } else {
        return getHttpSolrClient(baseUrl + "/" + DEFAULT_TEST_CORENAME);
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected String buildUrl(int port) {
    return buildUrl(port, context);
  }

  protected static void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }// add random fields to the documet before indexing

  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    addRandFields(doc);
    indexDoc(doc);
  }
  
  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    addFields(sdoc, getRandFields(getFieldNames(), getRandValues()));
    return sdoc;
  }

  protected void index(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    indexDoc(doc);
  }

  /**
   * Indexes the document in both the control client, and a randomly selected client
   */
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    controlClient.add(doc);
    if (shardCount == 0) {//mostly for temp debugging
      return;
    }
    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % clients.size();
    SolrClient client = clients.get(which);
    client.add(doc);
  }
  
  /**
   * Indexes the document in both the control client and the specified client asserting
   * that the responses are equivalent
   */
  protected UpdateResponse indexDoc(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateResponse controlRsp = add(controlClient, params, sdocs);
    UpdateResponse specificRsp = add(client, params, sdocs);
    compareSolrResponses(specificRsp, controlRsp);
    return specificRsp;
  }

  protected UpdateResponse add(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams(params));
    for (SolrInputDocument sdoc : sdocs) {
      ureq.add(sdoc);
    }
    return ureq.process(client);
  }

  protected UpdateResponse del(SolrClient client, SolrParams params, Object... ids) throws IOException, SolrServerException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams(params));
    for (Object id: ids) {
      ureq.deleteById(id.toString());
    }
    return ureq.process(client);
  }

  protected UpdateResponse delQ(SolrClient client, SolrParams params, String... queries) throws IOException, SolrServerException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams(params));
    for (String q: queries) {
      ureq.deleteByQuery(q);
    }
    return ureq.process(client);
  }

  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);

    SolrClient client = clients.get(serverNumber);
    client.add(doc);
  }

  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    for (SolrClient client : clients) {
      client.deleteByQuery(q);
    }
  }// serial commit...

  protected void commit() throws Exception {
    controlClient.commit();
    for (SolrClient client : clients) {
      client.commit();
    }
  }

  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException, IOException {
    // query a random server
    int which = r.nextInt(clients.size());
    SolrClient client = clients.get(which);
    QueryResponse rsp = client.query(params);
    return rsp;
  }

  /**
   * Sets distributed params.
   * Returns the QueryResponse from {@link #queryServer},
   */
  protected QueryResponse query(Object... q) throws Exception {
    return query(true, q);
  }

  /**
   * Sets distributed params.
   * Returns the QueryResponse from {@link #queryServer},
   */
  protected QueryResponse query(SolrParams params) throws Exception {
    return query(true, params);
  }

  /**
   * Returns the QueryResponse from {@link #queryServer}  
   */
  protected QueryResponse query(boolean setDistribParams, Object[] q) throws Exception {
    
    final ModifiableSolrParams params = createParams(q);
    return query(setDistribParams, params);
  }

  /**
   * Returns the QueryResponse from {@link #queryServer}  
   */
  protected QueryResponse query(boolean setDistribParams, SolrParams p) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams(p);

    // TODO: look into why passing true causes fails
    params.set("distrib", "false");
    final QueryResponse controlRsp = controlClient.query(params);
    validateControlData(controlRsp);

    if (shardCount == 0) {//mostly for temp debugging
      return controlRsp;
    }

    params.remove("distrib");
    if (setDistribParams) setDistributedParams(params);

    QueryResponse rsp = queryServer(params);

    compareResponses(rsp, controlRsp);

    if (stress > 0) {
      log.info("starting stress...");
      Thread[] threads = new Thread[nThreads];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread() {
          @Override
          public void run() {
            for (int j = 0; j < stress; j++) {
              int which = r.nextInt(clients.size());
              SolrClient client = clients.get(which);
              try {
                QueryResponse rsp = client.query(new ModifiableSolrParams(params));
                if (verifyStress) {
                  compareResponses(rsp, controlRsp);
                }
              } catch (SolrServerException | IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
        threads[i].start();
      }

      for (Thread thread : threads) {
        thread.join();
      }
    }
    return rsp;
  }
  
  public QueryResponse queryAndCompare(SolrParams params, SolrClient... clients) throws SolrServerException, IOException {
    return queryAndCompare(params, Arrays.<SolrClient>asList(clients));
  }
  public QueryResponse queryAndCompare(SolrParams params, Iterable<SolrClient> clients) throws SolrServerException, IOException {
    QueryResponse first = null;
    for (SolrClient client : clients) {
      QueryResponse rsp = client.query(new ModifiableSolrParams(params));
      if (first == null) {
        first = rsp;
      } else {
        compareResponses(first, rsp);
      }
    }

    return first;
  }

  public static boolean eq(String a, String b) {
    return a == b || (a != null && a.equals(b));
  }

  public static int flags(Map<String, Integer> handle, Object key) {
    if (key == null) return 0;
    if (handle == null) return 0;
    Integer f = handle.get(key);
    return f == null ? 0 : f;
  }

  @SuppressWarnings({"unchecked"})
  public static String compare(@SuppressWarnings({"rawtypes"})NamedList a,
                               @SuppressWarnings({"rawtypes"})NamedList b, int flags, Map<String, Integer> handle) {
//    System.out.println("resp a:" + a);
//    System.out.println("resp b:" + b);
    boolean ordered = (flags & UNORDERED) == 0;

    if (!ordered) {
      @SuppressWarnings({"rawtypes"})
      Map mapA = new HashMap(a.size());
      for (int i=0; i<a.size(); i++) {
        Object prev = mapA.put(a.getName(i), a.getVal(i));
      }

      @SuppressWarnings({"rawtypes"})
      Map mapB = new HashMap(b.size());
      for (int i=0; i<b.size(); i++) {
        Object prev = mapB.put(b.getName(i), b.getVal(i));
      }

      return compare(mapA, mapB, flags, handle);
    }

    int posa = 0, posb = 0;
    int aSkipped = 0, bSkipped = 0;

    for (; ;) {
      if (posa >= a.size() && posb >= b.size()) {
        break;
      }

      String namea = null, nameb = null;
      Object vala = null, valb = null;

      int flagsa = 0, flagsb = 0;
      while (posa < a.size()) {
        namea = a.getName(posa);
        vala = a.getVal(posa);
        posa++;
        flagsa = flags(handle, namea);
        if ((flagsa & SKIP) != 0) {
          namea = null; vala = null;
          aSkipped++;
          continue;
        }
        
        break;
      }

      while (posb < b.size()) {
        nameb = b.getName(posb);
        valb = b.getVal(posb);
        posb++;
        flagsb = flags(handle, nameb);
        if ((flagsb & SKIP) != 0) {
          nameb = null; valb = null;
          bSkipped++;
          continue;
        }
        if (eq(namea, nameb)) {
          break;
        }
        return "." + namea + "!=" + nameb + " (unordered or missing)";
        // if unordered, continue until we find the right field.
      }

      // ok, namea and nameb should be equal here already.
      if ((flagsa & SKIPVAL) != 0) continue;  // keys matching is enough

      String cmp = compare(vala, valb, flagsa, handle);
      if (cmp != null) return "." + namea + cmp;
    }


    if (a.size() - aSkipped != b.size() - bSkipped) {
      return ".size()==" + a.size() + "," + b.size() + " skipped=" + aSkipped + "," + bSkipped;
    }

    return null;
  }

  public static String compare1(@SuppressWarnings({"rawtypes"})Map a,
                                @SuppressWarnings({"rawtypes"})Map b,
                                int flags, Map<String, Integer> handle) {
    String cmp;

    for (Object keya : a.keySet()) {
      Object vala = a.get(keya);
      int flagsa = flags(handle, keya);
      if ((flagsa & SKIP) != 0) continue;
      if (!b.containsKey(keya)) {
        return "[" + keya + "]==null";
      }
      if ((flagsa & SKIPVAL) != 0) continue;
      Object valb = b.get(keya);
      cmp = compare(vala, valb, flagsa, handle);
      if (cmp != null) return "[" + keya + "]" + cmp;
    }
    return null;
  }

  public static String compare(@SuppressWarnings({"rawtypes"})Map a,
                               @SuppressWarnings({"rawtypes"})Map b,
                               int flags, Map<String, Integer> handle) {
    String cmp;
    cmp = compare1(a, b, flags, handle);
    if (cmp != null) return cmp;
    return compare1(b, a, flags, handle);
  }

  public static String compare(SolrDocument a, SolrDocument b, int flags, Map<String, Integer> handle) {
    return compare(a.getFieldValuesMap(), b.getFieldValuesMap(), flags, handle);
  }

  public static String compare(SolrDocumentList a, SolrDocumentList b, int flags, Map<String, Integer> handle) {
    boolean ordered = (flags & UNORDERED) == 0;

    String cmp;
    int f = flags(handle, "maxScore");
    if (f == 0) {
      cmp = compare(a.getMaxScore(), b.getMaxScore(), 0, handle);
      if (cmp != null) return ".maxScore" + cmp;
    } else if ((f & SKIP) == 0) { // so we skip val but otherwise both should be present
      assert (f & SKIPVAL) != 0;
      if (b.getMaxScore() != null) {
        if (a.getMaxScore() == null) {
          return ".maxScore missing";
        }
      }
    }

    cmp = compare(a.getNumFound(), b.getNumFound(), 0, handle);
    if (cmp != null) return ".numFound" + cmp;

    cmp = compare(a.getStart(), b.getStart(), 0, handle);
    if (cmp != null) return ".start" + cmp;

    cmp = compare(a.size(), b.size(), 0, handle);
    if (cmp != null) return ".size()" + cmp;

    // only for completely ordered results (ties might be in a different order)
    if (ordered) {
      for (int i = 0; i < a.size(); i++) {
        cmp = compare(a.get(i), b.get(i), 0, handle);
        if (cmp != null) return "[" + i + "]" + cmp;
      }
      return null;
    }

    // unordered case
    for (int i = 0; i < a.size(); i++) {
      SolrDocument doc = a.get(i);
      Object key = doc.getFirstValue("id");
      SolrDocument docb = null;
      if (key == null) {
        // no id field to correlate... must compare ordered
        docb = b.get(i);
      } else {
        for (int j = 0; j < b.size(); j++) {
          docb = b.get(j);
          if (key.equals(docb.getFirstValue("id"))) break;
        }
      }
      // if (docb == null) return "[id="+key+"]";
      cmp = compare(doc, docb, 0, handle);
      if (cmp != null) return "[id=" + key + "]" + cmp;
    }
    return null;
  }

  public static String compare(Object[] a, Object[] b, int flags, Map<String, Integer> handle) {
    if (a.length != b.length) {
      return ".length:" + a.length + "!=" + b.length;
    }
    for (int i = 0; i < a.length; i++) {
      String cmp = compare(a[i], b[i], flags, handle);
      if (cmp != null) return "[" + i + "]" + cmp;
    }
    return null;
  }

  public static String compare(Object a, Object b, int flags, Map<String, Integer> handle) {
    if (a == b) return null;
    if (a == null || b == null) return ":" + a + "!=" + b;

    if (a instanceof NamedList && b instanceof NamedList) {
      return compare((NamedList) a, (NamedList) b, flags, handle);
    }

    if (a instanceof SolrDocumentList && b instanceof SolrDocumentList) {
      return compare((SolrDocumentList) a, (SolrDocumentList) b, flags, handle);
    }

    if (a instanceof SolrDocument && b instanceof SolrDocument) {
      return compare((SolrDocument) a, (SolrDocument) b, flags, handle);
    }

    if (a instanceof Map && b instanceof Map) {
      return compare((Map) a, (Map) b, flags, handle);
    }

    if (a instanceof Object[] && b instanceof Object[]) {
      return compare((Object[]) a, (Object[]) b, flags, handle);
    }

    if (a instanceof byte[] && b instanceof byte[]) {
      if (!Arrays.equals((byte[]) a, (byte[]) b)) {
        return ":" + a + "!=" + b;
      }
      return null;
    }

    if (a instanceof List && b instanceof List) {
      return compare(((List) a).toArray(), ((List) b).toArray(), flags, handle);

    }

    // equivalent integer numbers
    if ((a instanceof Integer || a instanceof Long) && (b instanceof Integer || b instanceof Long)) {
      if (((Number)a).longValue() == ((Number)b).longValue()) {
        return null;
      } else {
        return ":" + a + "!=" + b;
      }
    }

    if ((flags & FUZZY) != 0) {
      if ((a instanceof Double && b instanceof Double)) {
        double aaa = ((Double) a).doubleValue();
        double bbb = ((Double) b).doubleValue();
        if (aaa == bbb || ((Double) a).isNaN() && ((Double) b).isNaN()) {
          return null;
        }
        if ((aaa == 0.0) || (bbb == 0.0)) {
            return ":" + a + "!=" + b;
        }

        double diff = Math.abs(aaa - bbb);
        // When stats computations are done on multiple shards, there may
        // be small differences in the results. Allow a small difference
        // between the result of the computations.

        double ratio = Math.max(Math.abs(diff / aaa), Math.abs(diff / bbb));
        if (ratio > DOUBLE_RATIO_LIMIT) {
          return ":" + a + "!=" + b;
        } else {
          return null;// close enough.
        }
      }
    }

    if (!(a.equals(b))) {
      return ":" + a + "!=" + b;
    }

    return null;
  }

  protected void compareSolrResponses(SolrResponse a, SolrResponse b) {
    // SOLR-3345: Checking QTime value can be skipped as there is no guarantee that the numbers will match.
    handle.put("QTime", SKIPVAL);
    // rf will be different since the control collection doesn't usually have multiple replicas
    handle.put("rf", SKIPVAL);
    String cmp = compare(a.getResponse(), b.getResponse(), flags, handle);
    if (cmp != null) {
      log.error("Mismatched responses:\n{}\n{}", a, b);
      Assert.fail(cmp);
    }
  }
  protected void compareResponses(QueryResponse a, QueryResponse b) {
    if (System.getProperty("remove.version.field") != null) {
      // we don't care if one has a version and the other doesnt -
      // control vs distrib
      // TODO: this should prob be done by adding an ignore on _version_ rather than mutating the responses?
      if (a.getResults() != null) {
        for (SolrDocument doc : a.getResults()) {
          doc.removeFields("_version_");
        }
      }
      if (b.getResults() != null) {
        for (SolrDocument doc : b.getResults()) {
          doc.removeFields("_version_");
        }
      }
    }
    { // we don't care if one has a warnings section in the header and the other doesn't - control vs distrib
      if (a.getHeader() != null) {
        a.getHeader().remove("warnings");
      }
      if (b.getHeader() != null) {
        b.getHeader().remove("warnings");
      }
    }
    compareSolrResponses(a, b);
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface ShardsRepeat {
    public abstract int min() default 1;
    public abstract int max() default DEFAULT_MAX_SHARD_COUNT;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface ShardsFixed {
    public abstract int num();
  }

  public class ShardsRepeatRule implements TestRule {

    private abstract class ShardsStatement extends Statement {
      abstract protected void callStatement() throws Throwable;

      @Override
      public void evaluate() throws Throwable {
        distribSetUp();
        if (! distribSetUpCalled) {
          Assert.fail("One of the overrides of distribSetUp does not propagate the call.");
        }
        try {
          callStatement();
        } finally {
          distribTearDown();
          if (! distribTearDownCalled) {
            Assert.fail("One of the overrides of distribTearDown does not propagate the call.");
          }
        }
      }
    }

    private class ShardsFixedStatement extends ShardsStatement {

      private final int numShards;
      private final Statement statement;

      private ShardsFixedStatement(int numShards, Statement statement) {
        this.numShards = numShards;
        this.statement = statement;
      }

      @Override
      @SuppressWarnings({"rawtypes"})
      public void callStatement() throws Throwable {
        RandVal.uniqueValues = new HashSet(); // reset random values
        fixShardCount(numShards);
        
        try {
          createServers(numShards);
          
          statement.evaluate();
        } finally {
          destroyServers();
        }
      }
    }

    private class ShardsRepeatStatement extends ShardsStatement {

      private final int min;
      private final int max;
      private final Statement statement;

      private ShardsRepeatStatement(int min, int max, Statement statement) {
        this.min = min;
        this.max = max;
        this.statement = statement;
      }

      @Override
      @SuppressWarnings({"rawtypes"})
      public void callStatement() throws Throwable {
        
        for (shardCount = min; shardCount <= max; shardCount++) {
          RandVal.uniqueValues = new HashSet(); //reset random values
          createServers(shardCount);
          try {
            statement.evaluate();
          } finally {
            destroyServers();
          }
        }
      }
    }

    @Override
    public Statement apply(Statement statement, Description description) {
      ShardsFixed fixed = description.getAnnotation(ShardsFixed.class);
      ShardsRepeat repeat = description.getAnnotation(ShardsRepeat.class);
      if (fixed != null && repeat != null) {
        throw new RuntimeException("ShardsFixed and ShardsRepeat annotations can't coexist");
      }
      else if (fixed != null) {
        return new ShardsFixedStatement(fixed.num(), statement);
      }
      else if (repeat != null) {
        return new ShardsRepeatStatement(repeat.min(), repeat.max(), statement);
      }
      else {
        return (isShardCountFixed ? new ShardsFixedStatement(shardCount, statement) :
          new ShardsRepeatStatement(1, DEFAULT_MAX_SHARD_COUNT, statement));
      }
    }
  }

  @Rule
  public ShardsRepeatRule repeatRule = new ShardsRepeatRule();

  public static Object[] getRandFields(String[] fields, RandVal[] randVals) {
    Object[] o = new Object[fields.length * 2];
    for (int i = 0; i < fields.length; i++) {
      o[i * 2] = fields[i];
      o[i * 2 + 1] = randVals[i].uval();
    }
    return o;
  }
  
  /**
   * Implementations can pre-test the control data for basic correctness before using it
   * as a check for the shard data.  This is useful, for instance, if a test bug is introduced
   * causing a spelling index not to get built:  both control &amp; shard data would have no results
   * but because they match the test would pass.  This method gives us a chance to ensure something
   * exists in the control data.
   */
  public void validateControlData(QueryResponse control) throws Exception {
    /* no-op */
  }

  @SuppressWarnings({"unchecked"})
  public static abstract class RandVal {
    @SuppressWarnings({"rawtypes"})
    public static Set uniqueValues = new HashSet();

    public abstract Object val();

    public Object uval() {
      for (; ;) {
        Object v = val();
        if (uniqueValues.add(v)) return v;
      }
    }
  }

  public static class RandDate extends RandVal {
    @Override
    public Object val() {
      long v = r.nextLong();
      Date d = new Date(v);
      return d.toInstant().toString();
    }
  }
  
  protected String getSolrXml() {
    return null;
  }
  
  /**
   * Given a directory that will be used as the SOLR_HOME for a jetty instance, seeds that 
   * directory with the contents of {@link #getSolrHome} and ensures that the proper {@link #getSolrXml} 
   * file is in place.
   */
  protected void seedSolrHome(File jettyHome) throws IOException {
    FileUtils.copyDirectory(new File(getSolrHome()), jettyHome);
    String solrxml = getSolrXml();
    if (solrxml != null) {
      FileUtils.copyFile(new File(getSolrHome(), solrxml), new File(jettyHome, "solr.xml"));
    }
  }

  /**
   * Given a directory that will be used as the <code>coreRootDirectory</code> for a jetty instance, 
   * Creates a core directory named {@link #DEFAULT_TEST_CORENAME} using a trivial
   * <code>core.properties</code> if this file does not already exist.
   *
   * @see #writeCoreProperties(Path,String)
   * @see #CORE_PROPERTIES_FILENAME
   */
  private void seedCoreRootDirWithDefaultTestCore(Path coreRootDirectory) throws IOException {
    // Kludgy and brittle with assumptions about writeCoreProperties, but i don't want to 
    // try to change the semantics of that method to ignore existing files
    Path coreDir = coreRootDirectory.resolve(DEFAULT_TEST_CORENAME);
    if (Files.notExists(coreDir.resolve(CORE_PROPERTIES_FILENAME))) {
      writeCoreProperties(coreDir, DEFAULT_TEST_CORENAME);
    } // else nothing to do, DEFAULT_TEST_CORENAME already exists
  }

  protected void setupJettySolrHome(File jettyHome) throws IOException {
    seedSolrHome(jettyHome);

    Files.createDirectories(jettyHome.toPath().resolve("cores").resolve("collection1"));
  }

  protected ModifiableSolrParams createParams(Object ... q) {
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    return params;
  }
}
