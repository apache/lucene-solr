package org.apache.solr;

/**
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper base class for distributed search test cases
 *
 * @since solr 1.5
 */
public abstract class BaseDistributedSearchTestCase extends SolrTestCaseJ4 {
  public static Random r = random;

  protected int shardCount = 4;
  /**
   * Sub classes can set this flag in their constructor to true if they
   * want to fix the number of shards to 'shardCount'
   *
   * The default is false which means that test will be executed with
   * 1, 2, 3, ....shardCount number of shards repeatedly
   */
  protected boolean fixShardCount = false;

  protected JettySolrRunner controlJetty;
  protected List<SolrServer> clients = new ArrayList<SolrServer>();
  protected List<JettySolrRunner> jettys = new ArrayList<JettySolrRunner>();
  protected String context = "/solr";
  protected String shards;
  protected String[] shardsArr;
  // Some ISPs redirect to their own web site for domains that don't exist, causing this to fail
  // protected String[] deadServers = {"does_not_exist_54321.com:33331/solr","localhost:33332/solr"};
  protected String[] deadServers = {"[::1]:33332/solr"};
  protected File testDir;
  protected SolrServer controlClient;

  // to stress with higher thread counts and requests, make sure the junit
  // xml formatter is not being used (all output will be buffered before
  // transformation to xml and cause an OOM exception).
  protected int stress = 2;
  protected boolean verifyStress = true;
  protected int nThreads = 3;


  public static int ORDERED = 1;
  public static int SKIP = 2;
  public static int SKIPVAL = 4;
  public static int UNORDERED = 8;

  protected int flags;
  protected Map<String, Integer> handle = new HashMap<String, Integer>();

  protected String id = "id";
  public static Logger log = LoggerFactory.getLogger(BaseDistributedSearchTestCase.class);
  
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

  /**
   * Perform the actual tests here
   *
   * @throws Exception on error
   */
  public abstract void doTest() throws Exception;

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
  
  @Override
  public void setUp() throws Exception {
    SolrTestCaseJ4.resetExceptionIgnores();  // ignore anything with ignore_exception in it
    super.setUp();
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    System.setProperty("solr.solr.home", getSolrHome());
    testDir = new File(TEMP_DIR,
            getClass().getName() + "-" + System.currentTimeMillis());
    testDir.mkdirs();
  }

  @Override
  public void tearDown() throws Exception {
    destroyServers();
    if (!AbstractSolrTestCase.recurseDelete(testDir)) {
      System.err.println("!!!! WARNING: best effort to remove " + testDir.getAbsolutePath() + " FAILED !!!!!");
    }
    super.tearDown();
  }

  protected void createServers(int numShards) throws Exception {
    controlJetty = createJetty(testDir, testDir + "/control/data");
    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    shardsArr = new String[numShards];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(testDir, testDir + "/shard" + i + "/data");
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      String shardStr = "localhost:" + j.getLocalPort() + context;
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
        List<String> replicas = new ArrayList<String>(Arrays.asList(deadServers));
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
    controlJetty.stop();
    for (JettySolrRunner jetty : jettys) jetty.stop();
    clients.clear();
    jettys.clear();
  }
  
  public JettySolrRunner createJetty(File baseDir, String dataDir) throws Exception {
    return createJetty(baseDir, dataDir, null, null);
  }

  public JettySolrRunner createJetty(File baseDir, String dataDir, String shardId) throws Exception {
    return createJetty(baseDir, dataDir, shardId, null);
  }
  
  public JettySolrRunner createJetty(File baseDir, String dataDir, String shardList, String solrConfigOverride) throws Exception {
    System.setProperty("solr.data.dir", dataDir);
    JettySolrRunner jetty = new JettySolrRunner("/solr", 0, solrConfigOverride);
    if(shardList != null) {
      System.setProperty("shard", shardList);
    }
    jetty.start();
    System.clearProperty("shard");
    return jetty;
  }
  
  protected SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      String url = "http://localhost:" + port + context;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer(url);
      s.setConnectionTimeout(100); // 1/10th sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }// add random fields to the documet before indexing

  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    addFields(doc, getRandFields(getFieldNames(), getRandValues()));
    indexDoc(doc);
  }

  protected void index(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    indexDoc(doc);
  }

  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    controlClient.add(doc);

    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % clients.size();
    SolrServer client = clients.get(which);
    client.add(doc);
  }

  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);

    SolrServer client = clients.get(serverNumber);
    client.add(doc);
  }

  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    for (SolrServer client : clients) {
      client.deleteByQuery(q);
    }
  }// serial commit...

  protected void commit() throws Exception {
    controlClient.commit();
    for (SolrServer client : clients) client.commit();
  }

  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {
    // query a random server
    int which = r.nextInt(clients.size());
    SolrServer client = clients.get(which);
    QueryResponse rsp = client.query(params);
    return rsp;
  }

  protected void query(Object... q) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }

    final QueryResponse controlRsp = controlClient.query(params);

    setDistributedParams(params);

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
              SolrServer client = clients.get(which);
              try {
                QueryResponse rsp = client.query(new ModifiableSolrParams(params));
                if (verifyStress) {
                  compareResponses(rsp, controlRsp);
                }
              } catch (SolrServerException e) {
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
  }

  public static boolean eq(String a, String b) {
    return a == b || (a != null && a.equals(b));
  }

  public static int flags(Map<String, Integer> handle, Object key) {
    if (handle == null) return 0;
    Integer f = handle.get(key);
    return f == null ? 0 : f;
  }

  public static String compare(NamedList a, NamedList b, int flags, Map<String, Integer> handle) {
    boolean ordered = (flags & UNORDERED) == 0;

    int posa = 0, posb = 0;
    int aSkipped = 0, bSkipped = 0;

    for (; ;) {
      if (posa >= a.size() || posb >= b.size()) {
        break;
      }

      String namea, nameb;
      Object vala, valb = null;

      int flagsa, flagsb;
      for (; ;) {
        namea = a.getName(posa);
        vala = a.getVal(posa);
        posa++;
        flagsa = flags(handle, namea);
        if ((flagsa & SKIP) != 0) {
          aSkipped++;
          continue;
        }
        break;
      }

      if (!ordered) posb = 0;  // reset if not ordered

      while (posb < b.size()) {
        nameb = b.getName(posb);
        valb = b.getVal(posb);
        posb++;
        flagsb = flags(handle, nameb);
        if ((flagsb & SKIP) != 0) {
          bSkipped++;
          continue;
        }
        if (eq(namea, nameb)) {
          break;
        }
        if (ordered) {
          return "." + namea + "!=" + nameb + " (unordered or missing)";
        }
        // if unordered, continue until we find the right field.
      }

      // ok, namea and nameb should be equal here already.
      if ((flagsa & SKIPVAL) != 0) continue;  // keys matching is enough

      String cmp = compare(vala, valb, flagsa, handle);
      if (cmp != null) return "." + namea + cmp;
    }


    if (a.size() - aSkipped != b.size() - bSkipped) {
      return ".size()==" + a.size() + "," + b.size() + "skipped=" + aSkipped + "," + bSkipped;
    }

    return null;
  }

  public static String compare1(Map a, Map b, int flags, Map<String, Integer> handle) {
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

  public static String compare(Map a, Map b, int flags, Map<String, Integer> handle) {
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
    if ((f & SKIPVAL) == 0) {
      cmp = compare(a.getMaxScore(), b.getMaxScore(), 0, handle);
      if (cmp != null) return ".maxScore" + cmp;
    } else {
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

    if (!(a.equals(b))) {
      return ":" + a + "!=" + b;
    }

    return null;
  }

  protected void compareResponses(QueryResponse a, QueryResponse b) {
    String cmp;
    cmp = compare(a.getResponse(), b.getResponse(), flags, handle);
    if (cmp != null) {
      log.info("Mismatched responses:\n" + a + "\n" + b);
      TestCase.fail(cmp);
    }
  }

  @Test
  public void testDistribSearch() throws Exception {
    if (fixShardCount) {
      createServers(shardCount);
      RandVal.uniqueValues = new HashSet(); //reset random values
      doTest();
      destroyServers();
    } else {
      for (int nServers = 1; nServers < shardCount; nServers++) {
        createServers(nServers);
        RandVal.uniqueValues = new HashSet(); //reset random values
        doTest();
        destroyServers();
      }
    }
  }

  public static Object[] getRandFields(String[] fields, RandVal[] randVals) {
    Object[] o = new Object[fields.length * 2];
    for (int i = 0; i < fields.length; i++) {
      o[i * 2] = fields[i];
      o[i * 2 + 1] = randVals[i].uval();
    }
    return o;
  }

  public static abstract class RandVal {
    public static Random r = random;
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
    public static TrieDateField df = new TrieDateField();

    @Override
    public Object val() {
      long v = r.nextLong();
      Date d = new Date(v);
      return df.toExternal(d);
    }
  }
}
