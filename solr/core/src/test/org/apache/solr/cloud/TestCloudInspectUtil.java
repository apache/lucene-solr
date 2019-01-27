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
package org.apache.solr.cloud;

import java.util.HashSet;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCloudInspectUtil extends SolrTestCaseJ4 {
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    
    super.tearDown();
  }
  
  @Test
  public void testCheckIfDiffIsLegal() throws Exception {
    Set<String> addFails = null;
    Set<String> deleteFails = null;
    SolrDocumentList a = getDocList("2", "3");
    SolrDocumentList b = getDocList("1");
    boolean legal = CloudInspectUtil.checkIfDiffIsLegal(a, b, "control", "cloud", addFails,
        deleteFails);
    
    assertFalse(legal);
    
    // ################################
    
    addFails = new HashSet<String>();
    deleteFails = new HashSet<String>();
    
    a = getDocList("2", "3", "4");
    b = getDocList("2", "3");
    addFails.add("4");
    
    legal = CloudInspectUtil.checkIfDiffIsLegal(a, b, "control", "cloud", addFails,
        deleteFails);
    
    assertTrue(legal);
    
    // ################################
    
    addFails = new HashSet<String>();
    deleteFails = new HashSet<String>();
    
    a = getDocList("2", "3", "4");
    b = getDocList("2", "3", "5");
    addFails.add("4");
    deleteFails.add("5");
    
    legal = CloudInspectUtil.checkIfDiffIsLegal(a, b, "control", "cloud", addFails,
        deleteFails);
    
    assertTrue(legal);
    
    // ################################
    
    addFails = new HashSet<String>();
    deleteFails = new HashSet<String>();
    
    a = getDocList("2", "3", "4");
    b = getDocList("2", "3", "5");
    addFails.add("4");
    deleteFails.add("6");
    
    legal = CloudInspectUtil.checkIfDiffIsLegal(a, b, "control", "cloud", addFails,
        deleteFails);
    
    assertFalse(legal);
    
    // ################################
    
    final HashSet<String> addFailsExpectEx = new HashSet<String>();
    final HashSet<String> deleteFailsExpectEx = new HashSet<String>();
    
    final SolrDocumentList aExpectEx = getDocList("2", "3", "4");
    final SolrDocumentList bExpectEx = getDocList("2", "3", "4");

    expectThrows(IllegalArgumentException.class, "Expected exception because lists have no diff",
        () -> CloudInspectUtil.checkIfDiffIsLegal(aExpectEx, bExpectEx,
            "control", "cloud", addFailsExpectEx, deleteFailsExpectEx));
  }

  private SolrDocumentList getDocList(String ... ids) {
    SolrDocumentList list = new SolrDocumentList();
    for (String id : ids) {
      SolrDocument doc = new SolrDocument();
      doc.addField("id", id);
      list.add(doc);
    }
    return list;
  }
  
}
