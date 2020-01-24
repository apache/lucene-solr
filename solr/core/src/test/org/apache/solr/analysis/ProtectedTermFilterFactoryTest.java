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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ProtectedTermFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.BeforeClass;

public class ProtectedTermFilterFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-protected-term.xml");
  }

  public void testBasic() throws Exception {
    String text = "Wuthering FooBar distant goldeN ABC compote";
    Map<String,String> args = new HashMap<>();
    args.put("ignoreCase", "true");
    args.put("protected", "protected-1.txt,protected-2.txt");  // Protected: foobar, jaxfopbuz, golden, compote
    args.put("wrappedFilters", "lowercase");

    ResourceLoader loader = new SolrResourceLoader(TEST_PATH().resolve("collection1"));
    ProtectedTermFilterFactory factory = new ProtectedTermFilterFactory(args);
    factory.inform(loader);

    TokenStream ts = factory.create(whitespaceMockTokenizer(text));
    BaseTokenStreamTestCase.assertTokenStreamContents(ts,
        new String[] { "wuthering", "FooBar", "distant", "goldeN", "abc", "compote" });
  }

  public void testTwoWrappedFilters() {
    // Index-time: Filters: truncate:4 & lowercase.  Protected (ignoreCase:true): foobar, jaxfopbuz, golden, compote
    // Query-time: No filters
    assertU(adoc("id", "1", "prefix4_lower", "Wuthering FooBar distant goldeN ABC compote"));
    assertU(commit());

    assertQ(req("prefix4_lower:(+wuth +FooBar +dist +goldeN +abc +compote)")
        , "//result[@numFound=1]"
    );
  }

  public void testDuplicateFilters() {
    // Index-time: Filters: truncate:3 & reversestring & truncate:2.  Protected (ignoreCase:true): foobar, jaxfopbuz, golden, compote
    // Query-time: No filters
    assertU(adoc("id", "1",
        "prefix3_rev_prefix2",            "Wuthering FooBar distant goldeN ABC compote",
        "prefix3_rev_prefix2_mixed_IDs",  "Wuthering FooBar distant goldeN ABC compote",
        "prefix3_rev_prefix2_mixed_case", "Wuthering FooBar distant goldeN ABC compote"));
    assertU(commit());

    assertQ(req("prefix3_rev_prefix2:(+tu +FooBar +si +goldeN +CB +compote)")
        , "//result[@numFound=1]"
    );
    assertQ(req("prefix3_rev_prefix2_mixed_IDs:(+tu +FooBar +si +goldeN +CB +compote)")
        , "//result[@numFound=1]"
    );
    assertQ(req("prefix3_rev_prefix2_mixed_case:(+tu +FooBar +si +goldeN +CB +compote)")
        , "//result[@numFound=1]"
    );
  }
}
