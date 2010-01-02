package org.apache.solr.cloud;

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

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.SolrIndexWriter;


/**
 * TODO: assert config came from ZooKeeper
 *
 */
public class BasicZkTest extends AbstractZkTestCase {
  public void testBasic() throws Exception {
    // test using ZooKeeper
    assertTrue("Not using ZooKeeper", h.getCoreContainer().isZooKeeperAware());
    
    // test merge factor picked up
    SolrCore core = h.getCore();
    SolrIndexWriter writer = new SolrIndexWriter("testWriter", core
        .getNewIndexDir(), core.getDirectoryFactory(), false, core.getSchema(),
        core.getSolrConfig().mainIndexConfig, core.getDeletionPolicy());
    assertEquals("Mergefactor was not picked up", writer.getMergeFactor(), 8);

    lrf.args.put("version", "2.0");
    assertQ("test query on empty index", req("qlkciyopsbgzyvkylsjhchghjrdf"),
        "//result[@numFound='0']");

    // test escaping of ";"
    assertU("deleting 42 for no reason at all", delI("42"));
    assertU("adding doc#42", adoc("id", "42", "val_s", "aa;bb"));
    assertU("does commit work?", commit());

    assertQ("backslash escaping semicolon", req("id:42 AND val_s:aa\\;bb"),
        "//*[@numFound='1']", "//int[@name='id'][.='42']");

    assertQ("quote escaping semicolon", req("id:42 AND val_s:\"aa;bb\""),
        "//*[@numFound='1']", "//int[@name='id'][.='42']");

    assertQ("no escaping semicolon", req("id:42 AND val_s:aa"),
        "//*[@numFound='0']");

    assertU(delI("42"));
    assertU(commit());
    assertQ(req("id:42"), "//*[@numFound='0']");

    // test allowDups default of false

    assertU(adoc("id", "42", "val_s", "AAA"));
    assertU(adoc("id", "42", "val_s", "BBB"));
    assertU(commit());
    assertQ(req("id:42"), "//*[@numFound='1']", "//str[.='BBB']");
    assertU(adoc("id", "42", "val_s", "CCC"));
    assertU(adoc("id", "42", "val_s", "DDD"));
    assertU(commit());
    assertQ(req("id:42"), "//*[@numFound='1']", "//str[.='DDD']");

    // test deletes
    String[] adds = new String[] { add(doc("id", "101"), "allowDups", "false"),
        add(doc("id", "101"), "allowDups", "false"),
        add(doc("id", "105"), "allowDups", "true"),
        add(doc("id", "102"), "allowDups", "false"),
        add(doc("id", "103"), "allowDups", "true"),
        add(doc("id", "101"), "allowDups", "false"), };
    for (String a : adds) {
      assertU(a, a);
    }
    assertU(commit());

    // test maxint
    assertQ(req("q", "id:[100 TO 110]", "rows", "2147483647"),
        "//*[@numFound='4']");

    // test big limit
    assertQ(req("q", "id:[100 TO 111]", "rows", "1147483647"),
        "//*[@numFound='4']");

    assertQ(req("id:[100 TO 110]"), "//*[@numFound='4']");
    assertU(delI("102"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]"), "//*[@numFound='3']");
    assertU(delI("105"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]"), "//*[@numFound='2']");
    assertU(delQ("id:[100 TO 110]"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]"), "//*[@numFound='0']");
    
    //nocommit
    System.out.println("search nodes:" + h.getCoreContainer().getZooKeeperController().getSearchNodes());
  }
}
