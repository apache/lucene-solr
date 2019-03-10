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

import java.io.IOException;
import java.io.StringWriter;
import org.apache.solr.SolrTestCase;

/** Test (some of the) character escaping functions of the XML class
 */

public class TestXMLEscaping extends SolrTestCase {
  private void doSimpleTest(String input,String expectedOutput) throws IOException {
    final StringWriter sw = new StringWriter();
    XML.escapeCharData(input, sw);
    final String result = sw.toString();
    assertEquals("Escaped output does not match expected value",expectedOutput,result);
  }
  
  public void testNoEscape() throws IOException {
    doSimpleTest("Bonnie","Bonnie");
  }
  
  public void testAmpAscii() throws IOException {
    doSimpleTest("Bonnie & Clyde","Bonnie &amp; Clyde");
  }

  public void testAmpAndTagAscii() throws IOException {
    doSimpleTest("Bonnie & Cl<em>y</em>de","Bonnie &amp; Cl&lt;em&gt;y&lt;/em&gt;de");
  }

  public void testAmpWithAccents() throws IOException {
    // 00e9 is unicode eacute
    doSimpleTest("Les \u00e9v\u00e9nements chez Bonnie & Clyde","Les \u00e9v\u00e9nements chez Bonnie &amp; Clyde");
  }

  public void testAmpDotWithAccents() throws IOException {
    // 00e9 is unicode eacute
    doSimpleTest("Les \u00e9v\u00e9nements chez Bonnie & Clyde.","Les \u00e9v\u00e9nements chez Bonnie &amp; Clyde.");
  }

  public void testAmpAndTagWithAccents() throws IOException {
    // 00e9 is unicode eacute
    doSimpleTest("Les \u00e9v\u00e9nements <chez/> Bonnie & Clyde","Les \u00e9v\u00e9nements &lt;chez/&gt; Bonnie &amp; Clyde");
  }

  public void testGt() throws IOException {
    doSimpleTest("a ]]> b","a ]]&gt; b");
  }
}



