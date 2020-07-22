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
package org.apache.solr.common.util;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.junit.Test;

public class TestZkMaintenanceUtils extends SolrTestCaseJ4 {
  @Test
  public void testPaths() {
    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent(null));

    assertEquals("Unexpected path construction"
        , "this/is/a"
        , ZkMaintenanceUtils.getZkParent("this/is/a/path"));

    assertEquals("Unexpected path construction"
        , "/root"
        , ZkMaintenanceUtils.getZkParent("/root/path/"));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent("/"));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent(""));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent("noslashesinstring"));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent("/leadingslashonly"));

  }
}
