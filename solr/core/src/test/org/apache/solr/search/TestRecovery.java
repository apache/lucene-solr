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
package org.apache.solr.search;


import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRecovery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema12.xml");
  }

  @Test
  public void testLogReplay() throws Exception {
    DirectUpdateHandler2.commitOnClose = false;

    clearIndex();
    assertU(commit());

    assertU(adoc("id","1"));
    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    h.close();

    createCore();

    // TODO: how to verify that previous close didn't do a commit
    // This should normally succeed, but isn't guaranteed to
    // assertJQ(req("q","id:1") ,"/response/numFound==0");

    // TODO: how to wait until recovery has completed?
    Thread.sleep(2000);

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

  }

}