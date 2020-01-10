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
package org.apache.solr.util;

import java.util.Locale;

import org.apache.solr.SolrTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTestInjection extends SolrTestCase {
  
  @BeforeClass
  public static void beforeClass() {
  
  }
  
  @AfterClass
  public static void cleanup() {
    TestInjection.reset();
  }
  
  public void testBasics() {
    TestInjection.failReplicaRequests = "true:100";

    Exception e = expectThrows(Exception.class, TestInjection::injectFailReplicaRequests);
    assertFalse("Should not fail based on bad syntax",
        e.getMessage().toLowerCase(Locale.ENGLISH).contains("bad syntax"));
    
    TestInjection.failReplicaRequests = "true:00";
    for (int i = 0; i < 100; i++) {
      // should never fail
      TestInjection.injectFailReplicaRequests();
      
    }
  }
  
  public void testBadSyntax() {
    testBadSyntax("true/10");
    testBadSyntax("boo:100");
    testBadSyntax("false:100f");
    testBadSyntax("TRUE:0:");
  }
  
  public void testGoodSyntax() {
    testGoodSyntax("true:10");
    testGoodSyntax("true:100");
    testGoodSyntax("false:100");
    testGoodSyntax("TRUE:0");
    testGoodSyntax("TRUE:00");
    testGoodSyntax("TRUE:000");
    testGoodSyntax("FALSE:50");
    testGoodSyntax("FAlsE:99");
    
  }

  public void testBadSyntax(String syntax) {
    TestInjection.failReplicaRequests = syntax;
    Exception e = expectThrows(Exception.class, TestInjection::injectFailReplicaRequests);
    assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("bad syntax"));
  }
  
  public void testGoodSyntax(String syntax) {
    TestInjection.failReplicaRequests = syntax;

    try {
      TestInjection.injectFailReplicaRequests();
    } catch (Exception e) {
      // we can fail, but should not be for bad syntax
      assertFalse(e.getMessage().toLowerCase(Locale.ENGLISH).contains("bad syntax"));
    }
  }

  public void testUsingConsistentRandomization() {
    assertSame(random(), TestInjection.random());
  }
}
