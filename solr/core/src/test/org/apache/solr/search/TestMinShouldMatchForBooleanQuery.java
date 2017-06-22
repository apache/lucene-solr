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

import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;

/**
 *
 */
public class TestMinShouldMatchForBooleanQuery extends AbstractSolrTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public void testMinShouldMatchForBooleanQuery() {
    String f = "name";  // name is whitespace tokenized

    assertU(adoc("id", "1",  f, "A B C D"));
    assertU(adoc("id", "2",  f, "A B C"));
    assertU(adoc("id", "3",  f, "A B"));
    assertU(commit());

    assertQ("without mm we expect 3 documents to match",
        req("name:(A B C D)"),
        "//result[@numFound='3']"
    );

    assertQ("with mm=3 we expect 2 documents to match",
        req("{!mm=3}name:(A B C D)"),
        "//result[@numFound='2']"
    );

    assertQ("with mm=80% in q we expect 2 documents to match",
        req("{!mm=80%}name:(A B C D)"),
        "//result[@numFound='2']"
    );

    assertQ("with mm=80% in fq we expect 2 documents to match",
        req("q", "*:*",
            "fq","{!mm=80%}name:(A B C D)"),
        "//result[@numFound='2']"
    );

    assertQ("with mm<50% 6<70% we expect 2 documents to match",
        req("q", "*:*",
            "fq","{!mm='3<50% 6<70%'}name:(A B C D E F)"),
        "//result[@numFound='2']"
    );

  }

}
