package org.apache.solr.core;

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
 * 
 */

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that checks that long running queries are exited by Solr using the
 * SolrQueryTimeoutImpl implementation.
 */
public class ExitableDirectoryReaderTest extends SolrTestCaseJ4 {
  
  static int NUM_DOCS_PER_TYPE = 100;
  static final String assertionString = "//result[@numFound='"+ (NUM_DOCS_PER_TYPE - 1) + "']";

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig-nocache-with-delaying-searchcomponent.xml", "schema12.xml");
    createIndex();
  }

  public static void createIndex() {
    int counter = 1;
    
    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ )
      assertU(adoc("id", Integer.toString(counter), "name", "a" + counter));

    counter++;
    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ )
      assertU(adoc("id", Integer.toString(counter), "name", "b" + counter));

    counter++;
    for(; counter % NUM_DOCS_PER_TYPE != 0; counter++ )
      assertU(adoc("id", Integer.toString(counter), "name", "dummy term doc" + counter));

    assertU(commit());
  }

  @Test
  public void testPrefixQuery() {
    assertQEx("", req("q","name:a*", "indent","true","timeAllowed","1")
        , SolrException.ErrorCode.BAD_REQUEST
    );

    assertQ(req("q","name:a*", "indent","true", "timeAllowed","10000"), assertionString);

    assertQEx("", req("q","name:a*", "indent","true", "timeAllowed","1")
        , SolrException.ErrorCode.BAD_REQUEST
    );

    assertQ(req("q","name:b*", "indent","true", "timeAllowed","10000"), assertionString);

    assertQ(req("q","name:b*", "indent","true", "timeAllowed",Long.toString(Long.MAX_VALUE)), assertionString);

    assertQ(req("q","name:b*", "indent","true", "timeAllowed","-7")); // negative timeAllowed should disable timeouts

    assertQ(req("q","name:b*", "indent","true"));
  }
  
  @Test
  public void testQueriesOnDocsWithMultipleTerms() {
    assertQ(req("q","name:dummy", "indent","true", "timeAllowed","10000"), assertionString);

    // This should pass even though this may take more than the 'timeAllowed' time, it doesn't take long
    // to iterate over 1 term (dummy).
    assertQ(req("q","name:dummy", "indent","true", "timeAllowed","10000"), assertionString);

    assertQEx("", req("q","name:doc*", "indent","true", "timeAllowed","1")
        , SolrException.ErrorCode.BAD_REQUEST
    );

  }
}


