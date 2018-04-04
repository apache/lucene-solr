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

import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

/**
 * Tests {@link ICUCollationField} with docValues.
 */
public class TestICUCollationFieldDocValues extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    String home = setupSolrHome();
    initCore("solrconfig.xml","schema.xml", home);
    // add some docs
    assertU(adoc("id", "1", "text", "\u0633\u0627\u0628"));
    assertU(adoc("id", "2", "text", "I WİLL USE TURKİSH CASING"));
    assertU(adoc("id", "3", "text", "ı will use turkish casıng"));
    assertU(adoc("id", "4", "text", "Töne"));
    assertU(adoc("id", "5", "text", "I W\u0049\u0307LL USE TURKİSH CASING"));
    assertU(adoc("id", "6", "text", "Ｔｅｓｔｉｎｇ"));
    assertU(adoc("id", "7", "text", "Tone"));
    assertU(adoc("id", "8", "text", "Testing"));
    assertU(adoc("id", "9", "text", "testing"));
    assertU(adoc("id", "10", "text", "toene"));
    assertU(adoc("id", "11", "text", "Tzne"));
    assertU(adoc("id", "12", "text", "\u0698\u0698"));
    assertU(commit());
  }
  
  /**
   * Ugly: but what to do? We want to test custom sort, which reads rules in as a resource.
   * These are largish files, and jvm-specific (as our documentation says, you should always
   * look out for jvm differences with collation).
   * So it's preferable to create this file on-the-fly.
   */
  public static String setupSolrHome() throws Exception {
    File tmpFile = createTempDir().toFile();
    
    // make data and conf dirs
    new File(tmpFile + "/collection1", "data").mkdirs();
    File confDir = new File(tmpFile + "/collection1", "conf");
    confDir.mkdirs();
    
    // copy over configuration files
    FileUtils.copyFile(getFile("analysis-extras/solr/collection1/conf/solrconfig-icucollate.xml"), new File(confDir, "solrconfig.xml"));
    FileUtils.copyFile(getFile("analysis-extras/solr/collection1/conf/schema-icucollate-dv.xml"), new File(confDir, "schema.xml"));
    
    // generate custom collation rules (DIN 5007-2), saving to customrules.dat
    RuleBasedCollator baseCollator = (RuleBasedCollator) Collator.getInstance(new ULocale("de", "DE"));

    String DIN5007_2_tailorings =
      "& ae , a\u0308 & AE , A\u0308"+
      "& oe , o\u0308 & OE , O\u0308"+
      "& ue , u\u0308 & UE , u\u0308";

    RuleBasedCollator tailoredCollator = new RuleBasedCollator(baseCollator.getRules() + DIN5007_2_tailorings);
    String tailoredRules = tailoredCollator.getRules();
    FileOutputStream os = new FileOutputStream(new File(confDir, "customrules.dat"));
    IOUtils.write(tailoredRules, os, "UTF-8");
    os.close();

    return tmpFile.getAbsolutePath();
  }

  /** 
   * Test termquery with german DIN 5007-1 primary strength.
   * In this case, ö is equivalent to o (but not oe) 
   */
  public void testBasicTermQuery() {
    assertQ("Collated TQ: ",
       req("fl", "id", "q", "sort_de:tone", "sort", "id asc" ),
              "//*[@numFound='2']",
              "//result/doc[1]/str[@name='id'][.=4]",
              "//result/doc[2]/str[@name='id'][.=7]"
    );
  }
  
  /** 
   * Test rangequery again with the DIN 5007-1 collator.
   * We do a range query of tone .. tp, in binary order this
   * would retrieve nothing due to case and accent differences.
   */
  public void testBasicRangeQuery() {
    assertQ("Collated RangeQ: ",
        req("fl", "id", "q", "sort_de:[tone TO tp]", "sort", "id asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=4]",
               "//result/doc[2]/str[@name='id'][.=7]"
     );
  }
  
  /** 
   * Test sort with a danish collator. ö is ordered after z
   */
  public void testBasicSort() {
    assertQ("Collated Sort: ",
        req("fl", "id", "q", "sort_da:[tz TO töz]", "sort", "sort_da asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=11]",
               "//result/doc[2]/str[@name='id'][.=4]"
     );
  }
  
  /** 
   * Test sort with an arabic collator. U+0633 is ordered after U+0698.
   * With a binary collator, the range would also return nothing.
   */
  public void testArabicSort() {
    assertQ("Collated Sort: ",
        req("fl", "id", "q", "sort_ar:[\u0698 TO \u0633\u0633]", "sort", "sort_ar asc" ),
               "//*[@numFound='2']",
               "//result/doc[1]/str[@name='id'][.=12]",
               "//result/doc[2]/str[@name='id'][.=1]"
     );
  }

  /** 
   * Test rangequery again with an Arabic collator.
   * Binary order would normally order U+0633 in this range.
   */
  public void testNegativeRangeQuery() {
    assertQ("Collated RangeQ: ",
        req("fl", "id", "q", "sort_ar:[\u062F TO \u0698]", "sort", "id asc" ),
               "//*[@numFound='0']"
     );
  }
  /**
   * Test canonical decomposition with turkish primary strength. 
   * With this sort order, İ is the uppercase form of i, and I is the uppercase form of ı.
   * We index a decomposed form of İ.
   */
  public void testCanonicalDecomposition() {
    assertQ("Collated TQ: ",
        req("fl", "id", "q", "sort_tr_canon:\"I Will Use Turkish Casıng\"", "sort", "id asc" ),
               "//*[@numFound='3']",
               "//result/doc[1]/str[@name='id'][.=2]",
               "//result/doc[2]/str[@name='id'][.=3]",
               "//result/doc[3]/str[@name='id'][.=5]"
     );
  }
  
  /** 
   * Test termquery with custom collator (DIN 5007-2).
   * In this case, ö is equivalent to oe (but not o) 
   */
  public void testCustomCollation() {
    assertQ("Collated TQ: ",
        req("fl", "id", "q", "sort_custom:toene"),
               "//*[@numFound='2']",
               "//result/doc/str[@name='id'][.=4]",
               "//result/doc/str[@name='id'][.=10]"
     );
  }
}
