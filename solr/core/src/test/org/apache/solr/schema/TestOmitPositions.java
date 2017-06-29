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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

public class TestOmitPositions extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    // add some docs
    assertU(adoc("id", "1", "nopositionstext", "this is a test this is only a test", "text", "just another test"));
    assertU(adoc("id", "2", "nopositionstext", "test test test test test test test test test test test test test", "text", "have a nice day"));
    assertU(commit());
  }
  
  public void testFrequencies() {
    // doc 2 should be ranked above doc 1
    assertQ("term query: ",
       req("fl", "id", "q", "nopositionstext:test"),
              "//*[@numFound='2']",
              "//result/doc[1]/str[@name='id'][.=2]",
              "//result/doc[2]/str[@name='id'][.=1]"
    );
  }
  
  public void testPositions() {
    // no results should be found:
    // lucene 3.x: silent failure
    // lucene 4.x: illegal state exception, field was indexed without positions
    
    ignoreException("was indexed without position data");
    try {
    assertQ("phrase query: ",
       req("fl", "id", "q", "nopositionstext:\"test test\""),
              "//*[@numFound='0']"
    );
    } catch (Exception expected) {
      assertTrue(expected.getCause() instanceof IllegalStateException);
      // in lucene 4.0, queries don't silently fail
    }
    resetExceptionIgnores();
  }
}
