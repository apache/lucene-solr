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

import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

/**
 * A test of basic features using the minial legal solr schema.
 */
public class MinimalSchemaTest extends SolrTestCaseJ4 {
  /**
   * NOTE: we explicitly use the general 'solrconfig.xml' file here, in 
   * an attempt to test as many broad features as possible.
   *
   * Do not change this to point at some other "simpler" solrconfig.xml 
   * just because you want to add a new test case using solrconfig.xml, 
   * but your new testcase adds a feature that breaks this test.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solr/collection1/conf/solrconfig.xml","solr/collection1/conf/schema-minimal.xml");

    /* make sure some misguided soul doesn't inadvertently give us 
       a uniqueKey field and defeat the point of the tests
    */
    assertNull("UniqueKey Field isn't null", 
               h.getCore().getLatestSchema().getUniqueKeyField());

    lrf.args.put(CommonParams.VERSION,"2.2");

    assertNull("Simple assertion that adding a document works", h.validateUpdate(
            adoc("id",  "4055",
                 "subject", "Hoss",
                 "project", "Solr")));
    assertNull(h.validateUpdate(adoc("id",  "4056",
                 "subject", "Yonik",
                 "project", "Solr")));
    assertNull(h.validateUpdate(commit()));
    assertNull(h.validateUpdate(optimize()));

  }

  @Test
  public void testSimpleQueries() {

    assertQ("couldn't find subject hoss",
            req("subject:Hoss")
            ,"//result[@numFound=1]"
            ,"//str[@name='id'][.='4055']"
            );

    assertQ("couldn't find subject Yonik",
            req("subject:Yonik")
            ,"//result[@numFound=1]"
            ,"//str[@name='id'][.='4056']"
            );
  }

  /** SOLR-1371 */
  @Test
  public void testLuke() {
    
    assertQ("basic luke request failed",
            req("qt", "/admin/luke")
            ,"//int[@name='numDocs'][.='2']"
            );

    assertQ("luke show schema failed",
            req("qt", "/admin/luke",
                "show","schema")
            ,"//int[@name='numDocs'][.='2']"
            ,"//null[@name='uniqueKeyField']"
            );

  }


  /** 
   * Iterates over all (non "/update/*") handlers in the core and hits 
   * them with a request (using some simple params) to verify that they 
   * don't generate an error against the minimal schema
   */
  @Test
  public void testAllConfiguredHandlers() {
    Set<String> handlerNames = h.getCore().getRequestHandlers().keySet();
    for (String handler : handlerNames) {
      try {


        if (handler.startsWith("/update") ||
            handler.startsWith("/admin") ||
            handler.startsWith("/schema") ||
            handler.startsWith("/config") ||
            handler.startsWith("/mlt") ||
            handler.startsWith("/export") ||
            handler.startsWith("/graph") ||
            handler.startsWith("/sql") ||
            handler.startsWith("/stream") ||
            handler.startsWith("/terms") ||
            handler.startsWith("/analysis/")||
            handler.startsWith("/debug/") ||
            handler.startsWith("/replication")
            ) {
          continue;
        }

        assertQ("failure w/handler: '" + handler + "'",
                req("qt", handler,
                    // this should be fairly innocuous for any type of query
                    "q", "foo:bar",
                    "omitHeader", "false"
                )
                ,"//lst[@name='responseHeader']"
                );
      } catch (Exception e) {
        throw new RuntimeException("exception w/handler: '" + handler + "'", 
                                   e);
      }
    }
  }
}


