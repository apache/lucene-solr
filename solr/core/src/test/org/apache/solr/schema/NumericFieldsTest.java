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
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;


public class NumericFieldsTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema-numeric.xml");
  }

  static String[] types = new String[]{"int", "long", "float", "double", "date"};

  public static SolrInputDocument getDoc(String id, Integer number, String date) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    for (String t : types) {
      if ("date".equals(t)) {
        doc.addField(t, date);
        doc.addField(t + "_last", date);
        doc.addField(t + "_first", date);
      } else {
        doc.addField(t, number);
        doc.addField(t + "_last", number);
        doc.addField(t + "_first", number);
      }
    }
    return doc;
  }

  @Test
  public void testSortMissingFirstLast() {
    clearIndex();

    // NOTE: segments may be merged in any order, so we can't make any assumptions about
    // the relative order of M1 vs M2 unless we have a secondary sort
    assertU(adoc("id", "M1"));
    assertU(adoc(getDoc("+4", 4, "2011-04-04T00:00:00Z")));
    assertU(adoc(getDoc("+5", 5, "2011-05-05T00:00:00Z")));
    assertU(adoc(getDoc("-3", -3, "2011-01-01T00:00:00Z")));
    assertU(adoc("id", "M2"));
    assertU(commit());
    
    // 'normal' sorting.  Missing Values are 0
    String suffix = "";
    for (String t : types) {
      if ("date".equals(t)) {
        assertQ("Sorting Asc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[2]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[3]/str[@name='id'][.='-3']",
            "//result/doc[4]/str[@name='id'][.='+4']",
            "//result/doc[5]/str[@name='id'][.='+5']"
        );

        assertQ("Sorting Desc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='+5']",
            "//result/doc[2]/str[@name='id'][.='+4']",
            "//result/doc[3]/str[@name='id'][.='-3']",
            "//result/doc[4]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[5]/str[@name='id'][starts-with(.,'M')]"
        );
        
        assertQ("Sorting Asc w/secondary on id desc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc, id desc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='M2']",
            "//result/doc[2]/str[@name='id'][.='M1']",
            "//result/doc[3]/str[@name='id'][.='-3']",
            "//result/doc[4]/str[@name='id'][.='+4']",
            "//result/doc[5]/str[@name='id'][.='+5']"
        );

        assertQ("Sorting Desc w/secondary on id asc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc, id asc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='+5']",
            "//result/doc[2]/str[@name='id'][.='+4']",
            "//result/doc[3]/str[@name='id'][.='-3']",
            "//result/doc[4]/str[@name='id'][.='M1']",
            "//result/doc[5]/str[@name='id'][.='M2']"
        );
      } else {
        assertQ("Sorting Asc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='-3']",
            "//result/doc[2]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[3]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[4]/str[@name='id'][.='+4']",
            "//result/doc[5]/str[@name='id'][.='+5']"
        );

        assertQ("Sorting Desc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='+5']",
            "//result/doc[2]/str[@name='id'][.='+4']",
            "//result/doc[3]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[4]/str[@name='id'][starts-with(.,'M')]",
            "//result/doc[5]/str[@name='id'][.='-3']"
        );
        
        assertQ("Sorting Asc w/secondary on id desc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc, id desc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='-3']",
            "//result/doc[2]/str[@name='id'][.='M2']",
            "//result/doc[3]/str[@name='id'][.='M1']",
            "//result/doc[4]/str[@name='id'][.='+4']",
            "//result/doc[5]/str[@name='id'][.='+5']"
        );

        assertQ("Sorting Desc w/secondary on id asc: " + t + suffix,
            req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc, id asc"),
            "//*[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='+5']",
            "//result/doc[2]/str[@name='id'][.='+4']",
            "//result/doc[3]/str[@name='id'][.='M1']",
            "//result/doc[4]/str[@name='id'][.='M2']",
            "//result/doc[5]/str[@name='id'][.='-3']"
        );

      }
    }


    // sortMissingLast = true
    suffix = "_last";
    for (String t : types) {
      assertQ("Sorting Asc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='-3']",
          "//result/doc[2]/str[@name='id'][.='+4']",
          "//result/doc[3]/str[@name='id'][.='+5']",
          "//result/doc[4]/str[@name='id'][starts-with(.,'M')]",
          "//result/doc[5]/str[@name='id'][starts-with(.,'M')]"
      );

      assertQ("Sorting Desc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='+5']",
          "//result/doc[2]/str[@name='id'][.='+4']",
          "//result/doc[3]/str[@name='id'][.='-3']",
          "//result/doc[4]/str[@name='id'][starts-with(.,'M')]",
          "//result/doc[5]/str[@name='id'][starts-with(.,'M')]"
      );

      assertQ("Sorting Asc w/secondary on id desc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc, id desc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='-3']",
          "//result/doc[2]/str[@name='id'][.='+4']",
          "//result/doc[3]/str[@name='id'][.='+5']",
          "//result/doc[4]/str[@name='id'][.='M2']",
          "//result/doc[5]/str[@name='id'][.='M1']"
      );

      assertQ("Sorting Desc w/secondary on id asc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc, id asc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='+5']",
          "//result/doc[2]/str[@name='id'][.='+4']",
          "//result/doc[3]/str[@name='id'][.='-3']",
          "//result/doc[4]/str[@name='id'][.='M1']",
          "//result/doc[5]/str[@name='id'][.='M2']"
      );
    }

    // sortMissingFirst = true
    suffix = "_first";
    for (String t : types) {
      assertQ("Sorting Asc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][starts-with(.,'M')]",
          "//result/doc[2]/str[@name='id'][starts-with(.,'M')]",
          "//result/doc[3]/str[@name='id'][.='-3']",
          "//result/doc[4]/str[@name='id'][.='+4']",
          "//result/doc[5]/str[@name='id'][.='+5']"
      );

      assertQ("Sorting Desc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][starts-with(.,'M')]",
          "//result/doc[2]/str[@name='id'][starts-with(.,'M')]",
          "//result/doc[3]/str[@name='id'][.='+5']",
          "//result/doc[4]/str[@name='id'][.='+4']",
          "//result/doc[5]/str[@name='id'][.='-3']"
      );

      assertQ("Sorting Asc w/secondary on id desc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " asc, id desc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='M2']",
          "//result/doc[2]/str[@name='id'][.='M1']",
          "//result/doc[3]/str[@name='id'][.='-3']",
          "//result/doc[4]/str[@name='id'][.='+4']",
          "//result/doc[5]/str[@name='id'][.='+5']"
      );

      assertQ("Sorting Desc w/secondary on id asc: " + t + suffix,
          req("fl", "id", "q", "*:*", "sort", (t + suffix) + " desc, id asc"),
          "//*[@numFound='5']",
          "//result/doc[1]/str[@name='id'][.='M1']",
          "//result/doc[2]/str[@name='id'][.='M2']",
          "//result/doc[3]/str[@name='id'][.='+5']",
          "//result/doc[4]/str[@name='id'][.='+4']",
          "//result/doc[5]/str[@name='id'][.='-3']"
      );

    }
  }
}
