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
package org.apache.solr.analysis;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestFoldingMultitermExtrasQuery extends SolrTestCaseJ4 {

  public String getCoreName() {
    return "basic";
  }

  @BeforeClass
  public static void beforeTests() throws Exception {
    File testHome = createTempDir().toFile();
    FileUtils.copyDirectory(getFile("analysis-extras/solr"), testHome);
    initCore("solrconfig-icucollate.xml","schema-folding-extra.xml", testHome.getAbsolutePath());

    int idx = 1;
    // ICUFoldingFilterFactory
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "BadMagicICUFolding"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "Ruß"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "ΜΆΪΟΣ"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "Μάϊος"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "résumé"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "re\u0301sume\u0301"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "ELİF"));
    assertU(adoc("id", Integer.toString(idx++), "content_icufolding", "eli\u0307f"));

    // ICUNormalizer2FilterFactory

    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "BadMagicICUFolding"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "Ruß"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "ΜΆΪΟΣ"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "Μάϊος"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "résumé"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "re\u0301sume\u0301"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "ELİF"));
    assertU(adoc("id", Integer.toString(idx++), "content_icunormalizer2", "eli\u0307f"));

    // ICUTransformFilterFactory
    assertU(adoc("id", Integer.toString(idx++), "content_icutransform", "Российская"));

    assertU(commit());
  }

  @Test
  public void testICUFolding() {
    assertQ(req("q", "content_icufolding:BadMagicicuFold*"), "//result[@numFound='1']");
    assertQ(req("q", "content_icufolding:rU*"), "//result[@numFound='1']");
    assertQ(req("q", "content_icufolding:Re*Me"), "//result[@numFound='2']");
    assertQ(req("q", "content_icufolding:RE\u0301su*"), "//result[@numFound='2']");
    assertQ(req("q", "content_icufolding:El*"), "//result[@numFound='2']");
  }
  @Test
  public void testICUNormalizer2() {
    assertQ(req("q", "content_icunormalizer2:BadMagicicuFold*"), "//result[@numFound='1']");
    assertQ(req("q", "content_icunormalizer2:RU*"), "//result[@numFound='1']");
    assertQ(req("q", "content_icunormalizer2:Μάϊ*"), "//result[@numFound='2']");
    assertQ(req("q", "content_icunormalizer2:re\u0301Su*"), "//result[@numFound='2']");
    assertQ(req("q", "content_icunormalizer2:eL*"), "//result[@numFound='2']");
  }
  
  public void testICUTransform() {
    assertQ(req("q", "content_icutransform:Росс*"), "//result[@numFound='1']");
  }
}
