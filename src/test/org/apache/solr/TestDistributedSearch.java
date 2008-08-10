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

package org.apache.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.AbstractSolrTestCase;

import java.io.File;
import java.util.*;

import junit.framework.TestCase;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestDistributedSearch extends TestCase {
  Random r = new Random(0);
  File testDir;
  
  SolrServer controlClient;
  JettySolrRunner controlJetty;

  private List<SolrServer> clients = new ArrayList<SolrServer>();
  private List<JettySolrRunner> jettys = new ArrayList<JettySolrRunner>();
  String context = "/solr";
  String shards;


  String id="id";
  String t1="a_t";
  String i1="a_i";
  String oddField="oddField_s";
  String missingField="missing_but_valid_field_t";
  String invalidField="invalid_field_not_in_schema";


  @Override public void setUp() throws Exception
  {
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    testDir = new File(System.getProperty("java.io.tmpdir")
        + System.getProperty("file.separator")
        + getClass().getName() + "-" + System.currentTimeMillis());
    testDir.mkdirs();
  }

  @Override public void tearDown() throws Exception
  {
    destroyServers();
    AbstractSolrTestCase.recurseDelete(testDir);
  }


  private void createServers(int numShards) throws Exception {
    controlJetty = createJetty("control");
    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numShards; i++) {
      if (sb.length()>0) sb.append(',');
      JettySolrRunner j = createJetty("shard"+i);
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      sb.append("localhost:"+j.getLocalPort()+context);
    }

    shards = sb.toString();
  }

  private void destroyServers() throws Exception {
    controlJetty.stop();
    for (JettySolrRunner jetty : jettys) jetty.stop();
    clients.clear();
    jettys.clear();    
  }

  private JettySolrRunner createJetty(String dataDirName) throws Exception {
    File subDir = new File(testDir, dataDirName);
    subDir.mkdirs();
    System.setProperty("solr.data.dir", subDir.toString());
    
    JettySolrRunner jetty = new JettySolrRunner("/solr", 0);

    jetty.start();
    return jetty;
  }

  protected SolrServer createNewSolrServer(int port)
  {
    try {
      // setup the server...
      String url = "http://localhost:"+port+context;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer( url );
      s.setConnectionTimeout(100); // 1/10th sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }


  void index(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i=0; i<fields.length; i+=2) {
      doc.addField((String)(fields[i]), fields[i+1]);
    }
    controlClient.add(doc);

    int which = (doc.getField(id).toString().hashCode() &0x7fffffff) % clients.size();
    SolrServer client = clients.get(which);
    client.add(doc);
  }

  void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i=0; i<fields.length; i+=2) {
      doc.addField((String)(fields[i]), fields[i+1]);
    }
    controlClient.add(doc);

    int which = serverNumber;
    SolrServer client = clients.get(which);
    client.add(doc);
  }

  void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    for (SolrServer client : clients) {
      client.deleteByQuery(q);
    }
  }


  // serial commit...
  void commit() throws Exception {
    controlClient.commit();
    for (SolrServer client : clients) client.commit();    
  }

  // to stress with higher thread counts and requests, make sure the junit
  // xml formatter is not being used (all output will be buffered before
  // transformation to xml and cause an OOM exception).
  int stress = 2;
  boolean verifyStress = true;
  int nThreads = 3;


  void query(Object... q) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i=0; i<q.length; i+=2) {
      params.add(q[i].toString(), q[i+1].toString());
    }

    final QueryResponse controlRsp = controlClient.query(params);

    // query a random server
    params.set("shards", shards);
    int which = r.nextInt(clients.size());
    SolrServer client = clients.get(which);
    QueryResponse rsp = client.query(params);

    compareResponses(rsp, controlRsp);

    if (stress>0) {
      System.out.println("starting stress...");
      Thread[] threads = new Thread[nThreads];
      for (int i=0; i<threads.length; i++) {
        threads[i] = new Thread() {
          public void run() {
            for (int j=0; j<stress; j++) {
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


  private static int ORDERED=1;
  private static int SKIP=2;
  private static int SKIPVAL=4;
  private static int UNORDERED=8;


  public static boolean eq(String a, String b) {
    return a==b || (a != null && a.equals(b));
  }

  public static int flags(Map<String,Integer> handle, Object key) {
    if (handle==null) return 0;
    Integer f = handle.get(key);
    return f == null ? 0 : f;
  }

  public static String compare(NamedList a, NamedList b, int flags, Map<String,Integer> handle) {
    boolean ordered = (flags&UNORDERED) == 0;

    int posa = 0, posb = 0;
    int na = 0, nb = 0;

    for(;;) {
      if (posa >= a.size() || posb >= b.size()) {
        break;
      }

      String namea=null, nameb=null;
      Object vala=null, valb=null;

      int flagsa, flagsb;
      for (;;) {
        namea = a.getName(posa);
        vala = a.getVal(posa);
        posa++;
        flagsa = flags(handle, namea);
        if ((flagsa & SKIP) != 0) continue;
        na++;
        break;
      }

      if (!ordered) posb=0;  // reset if not ordered

      while (posb<b.size()) {
        nameb = b.getName(posb);
        valb = b.getVal(posb);
        posb++;
        flagsb = flags(handle, nameb);
        if ((flagsb & SKIP) != 0) continue;
        if (eq(namea, nameb)) {
          nb++;
          break;
        }
        if (ordered) {
          return "."+namea+"!="+nameb+" (unordered or missing)";
        }
        // if unordered, continue until we find the right field.
      }

      // ok, namea and nameb should be equal here already.
      if ((flagsa & SKIPVAL) != 0) continue;  // keys matching is enough

      String cmp = compare(vala, valb, flagsa, handle);
      if (cmp != null) return "."+namea+cmp;
    }


    if (na != nb) {
      return ".size()=="+na+","+nb;
    }

    return null;
  }

  private static String compare1(Map a, Map b, int flags, Map<String,Integer> handle) {
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

  public static String compare(Map a, Map b, int flags, Map<String,Integer> handle) {
    String cmp;
    cmp = compare1(a,b,flags,handle);
    if (cmp != null) return cmp;
    return compare1(b,a,flags,handle);
  }

  public static String compare(SolrDocument a, SolrDocument b, int flags, Map<String,Integer> handle) {
    return compare(a.getFieldValuesMap(), b.getFieldValuesMap(), flags, handle);    
  }

  public static String compare(SolrDocumentList a, SolrDocumentList b, int flags, Map<String,Integer> handle) {
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
    for (int i=0; i<a.size(); i++) {
      cmp = compare(a.get(i), b.get(i), 0, handle);
      if (cmp != null) return "["+i+"]"+cmp;
      }
      return null;
    }

    // unordered case
    for (int i=0; i<a.size(); i++) {
      SolrDocument doc = a.get(i);
      Object key = doc.getFirstValue("id");
      SolrDocument docb=null;
      if (key==null) {
        // no id field to correlate... must compare ordered
        docb = b.get(i);
      } else {
        for (int j=0; j<b.size(); j++) {
          docb = b.get(j);
          if (key.equals(docb.getFirstValue("id"))) break;
        }
      }
      // if (docb == null) return "[id="+key+"]";
      cmp = compare(doc, docb, 0, handle);
      if (cmp != null) return "[id="+key+"]" + cmp;
    }
    return null;
  }

  public static String compare(Object[] a, Object[] b, int flags, Map<String,Integer> handle) {
    if (a.length != b.length) {
      return ".length:"+a.length+"!="+b.length;
    }
    for (int i=0; i<a.length; i++) {
      String cmp = compare(a[i], b[i], flags, handle);
      if (cmp != null) return "["+i+"]"+cmp;
    }
    return null;
  }


  static String compare(Object a, Object b, int flags, Map<String,Integer> handle) {
    if (a==b) return null;
    if (a==null || b==null) return ":" +a + "!=" + b;

    if (a instanceof NamedList && b instanceof NamedList) {
      return compare((NamedList)a, (NamedList)b, flags, handle);
    }

    if (a instanceof SolrDocumentList && b instanceof SolrDocumentList) {
      return compare((SolrDocumentList)a, (SolrDocumentList)b, flags, handle);
    }

    if (a instanceof SolrDocument && b instanceof SolrDocument) {
      return compare((SolrDocument)a, (SolrDocument)b, flags, handle);
    }

    if (a instanceof Map && b instanceof Map) {
      return compare((Map)a, (Map)b, flags, handle);
    }

    if (a instanceof Object[] && b instanceof Object[]) {
      return compare((Object[])a, (Object[])b, flags, handle);
    }

    if (a instanceof byte[] && b instanceof byte[]) {
      if (!Arrays.equals((byte[])a, (byte[])b)) {
        return ":" + a + "!=" + b;
      }
      return null;
    }

    if (a instanceof List && b instanceof List) {
      return compare(((List)a).toArray(), ((List)b).toArray(), flags, handle);

    }

    if (!(a.equals(b))) {
      return ":" + a + "!=" + b;
    }

    return null;
  }


  void compareResponses(QueryResponse a, QueryResponse b) {
    String cmp;    
    cmp = compare(a.getResponse(), b.getResponse(), flags, handle);
    if (cmp != null) {
      System.out.println(a);
      System.out.println(b);
      TestCase.fail(cmp);
    }
  }

  int flags;
  Map<String, Integer> handle = new HashMap<String,Integer>();


  public void testDistribSearch() throws Exception {
    for (int nServers=1; nServers<4; nServers++) {
      createServers(nServers);
      doTest();
    }
  }

  public void doTest() throws Exception {
    del("*:*");
    index(id,1, i1, 100,t1,"now is the time for all good men"
            ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
    index(id,2, i1, 50 ,t1,"to come to the aid of their country.");
    index(id,3, i1, 2 ,t1,"how now brown cow");
    index(id,4, i1, -100 ,t1,"the quick fox jumped over the lazy dog");
    index(id,5, i1, 500 ,t1,"the quick fox jumped way over the lazy dog");
    index(id,6, i1, -600 ,t1,"humpty dumpy sat on a wall");
    index(id,7, i1, 123 ,t1,"humpty dumpy had a great fall");
    index(id,8, i1, 876 ,t1,"all the kings horses and all the kings men");
    index(id,9, i1, 7 ,t1,"couldn't put humpty together again");
    index(id,10, i1, 4321 ,t1,"this too shall pass");
    index(id,11, i1, -987 ,t1,"An eye for eye only ends up making the whole world blind.");
    index(id,12, i1, 379 ,t1,"Great works are performed, not by strength, but by perseverance.");
    index(id,13, i1, 232 ,t1,"no eggs on wall, lesson learned", oddField, "odd man out");

    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    handle.put("maxScore", SKIPVAL);
    query("q","{!func}"+i1);// does not expect maxScore. So if it comes ,ignore it. NamedListCodec.writeSolrDocumentList()
    //is agnostic of request params.
    handle.remove("maxScore");
    query("q","{!func}"+i1, "fl","*,score");  // even scores should match exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query("q","quick");
    query("q","all","fl","id","start","0");
    query("q","all","fl","foofoofoo","start","0");  // no fields in returned docs
    query("q","all","fl","id","start","100");

    handle.put("score", SKIPVAL);
    query("q","quick","fl","*,score");
    query("q","all","fl","*,score","start","1");
    query("q","all","fl","*,score","start","100");

    query("q","now their fox sat had put","fl","*,score",
            "hl","true","hl.fl",t1);

    query("q","now their fox sat had put","fl","foofoofoo",
            "hl","true","hl.fl",t1);


    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);

    query("q","now their fox sat had put","fl","*,score",
            "debugQuery", "true");

    query("q","matchesnothing","fl","*,score",
            "debugQuery", "true");    

    query("q","*:*", "rows",100, "facet","true", "facet.field",t1);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1,"facet.limit",1);
    query("q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.offset",1);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.mincount",2);

    // test faceting multiple things at once
    query("q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"
    ,"facet.field",t1);

    // test field that is valid in schema but missing in all shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2);
    // test field that is valid in schema and missing in some shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2);

    try {
      // test error produced for field that is invalid for schema
      query("q","*:*", "rows",100, "facet","true", "facet.field",invalidField, "facet.mincount",2);
      fail("SolrServerException expected for invalid field that is not in schema");
    } catch (SolrServerException ex) {
      // expected
    }

    // index the same document to two servers and make sure things
    // don't blow up.
    if (clients.size()>=2) {
      index(id,100, i1, 107 ,t1,"oh no, a duplicate!");
      for (int i=0; i<clients.size(); i++) {
        index_specific(i, id,100, i1, 107 ,t1,"oh no, a duplicate!");
      }
      commit();
      query("q","duplicate", "hl","true", "hl.fl", t1);
      query("q","fox duplicate horses", "hl","true", "hl.fl", t1);
      query("q","*:*", "rows",100);
    }

    // Thread.sleep(10000000000L);

    destroyServers();
  }


}



