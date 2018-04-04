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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

import java.io.File;

/**
 * Tests expert options of {@link ICUCollationField}.
 */
public class TestICUCollationFieldOptions extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    File testHome = createTempDir().toFile();
    FileUtils.copyDirectory(getFile("analysis-extras/solr"), testHome);
    initCore("solrconfig-icucollate.xml","schema-icucollateoptions.xml", testHome.getAbsolutePath());
    // add some docs
    assertU(adoc("id", "1", "text", "foo-bar"));
    assertU(adoc("id", "2", "text", "foo bar"));
    assertU(adoc("id", "3", "text", "foobar"));
    assertU(adoc("id", "4", "text", "foobar-10"));
    assertU(adoc("id", "5", "text", "foobar-9"));
    assertU(adoc("id", "6", "text", "resume"));
    assertU(adoc("id", "7", "text", "Résumé"));
    assertU(adoc("id", "8", "text", "Resume"));
    assertU(adoc("id", "9", "text", "résumé"));
    assertU(commit());
  }
  
  /*
   * Setting alternate=shifted to shift whitespace, punctuation and symbols
   * to quaternary level 
   */
  public void testIgnorePunctuation() { 
    assertQ("Collated TQ: ",
        req("fl", "id", "q", "sort_ignore_punctuation:foobar", "sort", "id asc" ),
               "//*[@numFound='3']",
               "//result/doc[1]/str[@name='id'][.=1]",
               "//result/doc[2]/str[@name='id'][.=2]",
               "//result/doc[3]/str[@name='id'][.=3]"
     );
  }
  
  /*
   * Setting alternate=shifted and variableTop to shift whitespace, but not 
   * punctuation or symbols, to quaternary level 
   */
  public void testIgnoreWhitespace() {
    assertQ("Collated TQ: ",
        req("fl", "id", "q", "sort_ignore_space:\"foo bar\"", "sort", "id asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=2]",
               "//result/doc[2]/str[@name='id'][.=3]"
     );
  }
  
  /*
   * Setting numeric to encode digits with numeric value, so that
   * foobar-9 sorts before foobar-10
   */
  public void testNumerics() {
    assertQ("Collated sort: ",
        req("fl", "id", "q", "id:[4 TO 5]", "sort", "sort_numerics asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=5]",
               "//result/doc[2]/str[@name='id'][.=4]"
     );
  }
  
  /*
   * Setting caseLevel=true to create an additional case level between
   * secondary and tertiary
   */
  public void testIgnoreAccentsButNotCase() {
    assertQ("Collated TQ: ",
        req("fl", "id", "q", "sort_ignore_accents:resume", "sort", "id asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=6]",
               "//result/doc[2]/str[@name='id'][.=9]"
     );
    
    assertQ("Collated TQ: ",
        req("fl", "id", "q", "sort_ignore_accents:Resume", "sort", "id asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=7]",
               "//result/doc[2]/str[@name='id'][.=8]"
     );
  }
  
  /*
   * Setting caseFirst=upper to cause uppercase strings to sort
   * before lowercase ones.
   */
  public void testUpperCaseFirst() {
    assertQ("Collated sort: ",
        req("fl", "id", "q", "id:6 OR id:8", "sort", "sort_uppercase_first asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=8]",
               "//result/doc[2]/str[@name='id'][.=6]"
     );
  }
}
