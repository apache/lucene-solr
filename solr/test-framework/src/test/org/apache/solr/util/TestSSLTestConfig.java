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

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.Constants;

import org.apache.solr.SolrTestCase;

public class TestSSLTestConfig extends SolrTestCase {

  /** Sanity check that our JVM version parsing logic seems correct */
  public void testIsOpenJdkJvmVersionKnownToHaveProblems() {
    final List<String> rel_suffixes = Arrays.asList("", "+42");
    final List<String> ea_suffixes = Arrays.asList("-ea", "-ea+42");
    final List<String> suffixes = Arrays.asList("", "+42", "-ea", "-ea+42");

    // as far as we know, any Java 8, 9 or 10 impl should be fine...
    for (String base : Arrays.asList("1.8", "1.8.0", "1.8.1", 
                                     "9", "9.0", "9.1", "9.0.0", "9.1.0", "9.1.1",
                                     "10", "10.0", "10.1", "10.0.0", "10.1.0", "10.1.1")) {
      for (String suffix : suffixes) {
        final String v = base + suffix;
        assertFalse(v, SSLTestConfig.isOpenJdkJvmVersionKnownToHaveProblems(v));
      }
    }

    // Known Problems start with Java 11...

    // java 11 releases below 11.0.3 were all bad...
    for (String bad : Arrays.asList("11", "11.0", "11.0.1", "11.0.2")) {
      for (String suffix : suffixes) {
        final String v = bad + suffix;
        assertTrue(v, SSLTestConfig.isOpenJdkJvmVersionKnownToHaveProblems(v));
      }
    }
    
    // ...but 11.0.3 or higher should be ok.
    for (String ok : Arrays.asList("11.0.3", "11.0.42", "11.1", "11.1.42")) {
      for (String suffix : suffixes) {
        final String v = ok + suffix;
        assertFalse(v, SSLTestConfig.isOpenJdkJvmVersionKnownToHaveProblems(v));
      }
    }
    
    // As far as we know/hope, all "official" java 12 and higher impls should be fine...
    for (String major : Arrays.asList("12", "13", "99")) {
      for (String minor : Arrays.asList("", ".0", ".42", ".0.42")) {
        for (String suffix : rel_suffixes) {
          final String v = major + minor + suffix;
          assertFalse(v, SSLTestConfig.isOpenJdkJvmVersionKnownToHaveProblems(v));
        }
      }
    }

    // ...but pre EA "testing" builds of 11, 12, and 13 are all definitely problematic...
    for (String major : Arrays.asList("11", "12", "13")) {
      for (String suffix : suffixes) {
        final String v = major + "-testing" + suffix;
        assertTrue(v, SSLTestConfig.isOpenJdkJvmVersionKnownToHaveProblems(v));
      }
    }

    // ...and all 13-ea builds (so far) have definitely been problematic.
    for (String suffix : ea_suffixes) {
      final String v = "13" + suffix;
      assertTrue(v, SSLTestConfig.isOpenJdkJvmVersionKnownToHaveProblems(v));
    }
    
  }

  public void testFailIfUserRunsTestsWithJVMThatHasKnownSSLBugs() {
    // NOTE: If there is some future JVM version, where all available "ea" builds are known to be buggy,
    // but we still want to be able to use for running tests (ie: via jenkins) to look for *other* bugs,
    // then those -ea versions can be "white listed" here...

    try {
      SSLTestConfig.assumeSslIsSafeToTest();
    } catch (org.junit.AssumptionViolatedException ave) {
      fail("Current JVM (" + Constants.JVM_NAME + " / " + Constants.JVM_VERSION +
           ") is known to have SSL Bugs.  Other tests that (explicitly or via randomization) " +
           " use SSL will be SKIPed");
    }
  }

  
}
