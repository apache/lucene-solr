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
package org.apache.solr.scripting.handler;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the ability to configure multiple query output writers, and select those
 * at query time.
 *
 */
public class OutputWriterTest extends SolrTestCaseJ4 {
    

    
    @BeforeClass
    public static void beforeClass() throws Exception {
      //initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
    initCore("solrconfig.xml","schema.xml");
     // initCore("solrconfig.xml", "schema.xml", getFile("scripting/solr").getAbsolutePath());
    }
    
    
  
    
    @Test
    public void testTrivialXsltWriter() throws Exception {
        lrf.args.put("wt", "xslt");
        lrf.args.put("tr", "dummy.xsl");
        String out = h.query(req("foo"));
        assertTrue(out.contains("DUMMY"));
    }
    
    @Test
    public void testTrivialXsltWriterInclude() throws Exception {
        lrf.args.put("wt", "xslt");
        lrf.args.put("tr", "dummy-using-include.xsl");
        String out = h.query(req("foo"));
        assertTrue(out.contains("DUMMY"));
    }

  
    
}
