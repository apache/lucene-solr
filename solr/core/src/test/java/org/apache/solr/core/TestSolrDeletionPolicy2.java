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
package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestSolrDeletionPolicy2 extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-delpolicy2.xml","schema.xml");
  }

  @Test
  public void testFakeDeletionPolicyClass() {

    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    assertTrue(delPolicy.getWrappedDeletionPolicy() instanceof FakeDeletionPolicy);

    FakeDeletionPolicy f = (FakeDeletionPolicy) delPolicy.getWrappedDeletionPolicy();

    assertTrue("value1".equals(f.getVar1()));
    assertTrue("value2".equals(f.getVar2()));

    assertU(adoc("id", String.valueOf(1),
            "name", "name" + String.valueOf(1)));


    assertTrue(System.getProperty("onInit").equals("test.org.apache.solr.core.FakeDeletionPolicy.onInit"));
    assertU(commit());
    assertQ("return all docs",
            req("id:[0 TO 1]"),
            "*[count(//doc)=1]"
    );


    assertTrue(System.getProperty("onCommit").equals("test.org.apache.solr.core.FakeDeletionPolicy.onCommit"));

    System.clearProperty("onInit");
    System.clearProperty("onCommit");
  }

}
