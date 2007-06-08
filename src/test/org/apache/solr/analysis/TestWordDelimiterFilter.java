/**
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

import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;
import org.apache.solr.request.SolrQueryRequest;

/**
 * New WordDelimiterFilter tests... most of the tests are in ConvertedLegacyTest
 */
public class TestWordDelimiterFilter extends AbstractSolrTestCase {
  public String getSchemaFile() { return "solr/conf/schema.xml"; }
  public String getSolrConfigFile() { return "solr/conf/solrconfig.xml"; }


  public void posTst(String v1, String v2, String s1, String s2) {
    assertU(adoc("id",  "42",
                 "subword", v1,
                 "subword", v2));
    assertU(commit());

    // there is a positionIncrementGap of 100 between field values, so
    // we test if that was maintained.
    assertQ("position increment lost",
            req("+id:42 +subword:\"" + s1 + ' ' + s2 + "\"~90")
            ,"//result[@numFound=0]"
    );
    assertQ("position increment lost",
            req("+id:42 +subword:\"" + s1 + ' ' + s2 + "\"~110")
            ,"//result[@numFound=1]"
    );
  }


  public void testRetainPositionIncrement() {
    posTst("foo","bar","foo","bar");
    posTst("-foo-","-bar-","foo","bar");
    posTst("foo","bar","-foo-","-bar-");

    posTst("123","456","123","456");
    posTst("/123/","/456/","123","456");

    posTst("/123/abc","qwe/456/","abc","qwe");

    posTst("zoo-foo","bar-baz","foo","bar");
    posTst("zoo-foo-123","456-bar-baz","foo","bar");
  }

  public void testNoGenerationEdgeCase() {
    assertU(adoc("id", "222", "numberpartfail", "123.123.123.123"));
  }

  public void testIgnoreCaseChange() {

    assertU(adoc("id",  "43",
                 "wdf_nocase", "HellO WilliAM",
                 "subword", "GoodBye JonEs"));
    assertU(commit());
    
    assertQ("no case change",
            req("wdf_nocase:(hell o am)")
            ,"//result[@numFound=0]"
    );
    assertQ("case change",
            req("subword:(good jon)")
            ,"//result[@numFound=1]"
    );
  }
}
