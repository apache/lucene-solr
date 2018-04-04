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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestComplexPhraseLeadingWildcard extends SolrTestCaseJ4 { 

  private static final String noReverseText = "three";
  private static final String withOriginal = "one";
  private static final String withoutOriginal = "two";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-reversed.xml");
    assertU(doc123(1, "one ever"));
    assertU(doc123(2, "once forever"));
                      
    assertU(doc123(7, "once slope forever"));
    assertU(doc123(8, "once again slope forever"));
    assertU(doc123(9, "forever once"));
    assertU(commit());
  }
  
  @Test
  public void testReverseWithOriginal() throws Exception {
    checkField(withOriginal);
    
  }

  // prefix query won't match without original tokens
  @Test
  public void testReverseWithoutOriginal() throws Exception {
    assertQ( "prefix query doesn't work without original term",
        req("q","{!complexphrase inOrder=true}\"on* for*\"",
            "df",withoutOriginal),
        expect());
    
    assertQ("postfix query works fine even without original",
        req("q","{!complexphrase inOrder=true}\"*nce *ver\"",
            "df",withoutOriginal),
        expect("2"));
  }
  
  @Test
  public void testWithoutReverse() throws Exception {
    checkField(noReverseText);
  }

  private void checkField(String field) {
    assertQ(
        req("q","{!complexphrase inOrder=true}\"on* *ver\"",
            "df",field,
            "indent","on",
            "debugQuery", "true"),
        expect("1","2"));
    
    assertQ(
        req("q","{!complexphrase inOrder=true}\"ON* *VER\"",
            "df",field), 
        expect("1","2"));
    
    assertQ(
        req("q","{!complexphrase inOrder=true}\"ON* *ver\"",
            "df",field), 
        expect("1","2"));
    
    assertQ(
        req("q","{!complexphrase inOrder=true}\"on* *ver\"~1",
            "df",field),
        expect("1","2","7"));
    
    assertQ("range works if reverse doesn't mess",
        req("q","{!complexphrase inOrder=true}\"on* [* TO a]\"",
            "df",field),
        expect());

    assertQ("range works if reverse doesn't mess",
        req("q","{!complexphrase inOrder=true}\"[on TO onZ] for*\"",
            "df",field),
        expect("2"));
  } 
  
  private static String doc123(int id, String text){
    return adoc("id",""+id, withOriginal, text, withoutOriginal, text, noReverseText, text);
  }
  
  private static String [] expect(String ...ids) {
    String[] xpathes = new String[ids.length+1];
    xpathes[0]= "//result[@numFound=" +ids.length+ "]";
    int i=1;
    for(String id : ids) {
      xpathes[i++] = "//doc/str[@name='id' and text()='"+id+"']";
    }
    return xpathes;
  }
}
