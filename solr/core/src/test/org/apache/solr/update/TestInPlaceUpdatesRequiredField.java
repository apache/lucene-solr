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

package org.apache.solr.update;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.update.TestInPlaceUpdatesStandalone.addAndAssertVersion;

public class TestInPlaceUpdatesRequiredField extends SolrTestCaseJ4  {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-inplace-required-field.xml");
  }

  @Test
  public void testUpdateFromTlog() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "name", "first", "inplace_updatable_int", 1), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='1']");

    // do an in place update that hits off the tlog
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1)); // new value should be 4
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:1", "fl", "*,[docid]"),
        "//result/doc[1]/int[@name='inplace_updatable_int'][.='4']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']");
  }
}
