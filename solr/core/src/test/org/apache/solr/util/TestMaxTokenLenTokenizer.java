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

package org.apache.solr.util;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * Tests for:
 * {@link org.apache.lucene.analysis.core.LetterTokenizerFactory}
 * {@link org.apache.lucene.analysis.core.KeywordTokenizerFactory}
 * {@link org.apache.lucene.analysis.core.WhitespaceTokenizerFactory}
 */

public class TestMaxTokenLenTokenizer extends SolrTestCaseJ4 {
  /* field names are used in accordance with the solrconfig and schema supplied */
  private static final String ID = "id";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema-tokenizer-test.xml");
  }

  public void testSingleFieldDiffAnalyzers() throws Exception {

    clearIndex();

    // using fields with definitions, different tokenizer factories respectively at index time and standard tokenizer at query time.

    updateJ("{\"add\":{\"doc\": {\"id\":1,\"letter\":\"letter\"}},\"commit\":{}}",null);
    updateJ("{\"add\":{\"doc\": {\"id\":3,\"whiteSpace\":\"whiteSpace in\"}},\"commit\":{}}",null);
    updateJ("{\"add\":{\"doc\": {\"id\":4,\"unicodeWhiteSpace\":\"unicode in\"}},\"commit\":{}}",null);
    updateJ("{\"add\":{\"doc\": {\"id\":5,\"keyword\":\"keyword\"}},\"commit\":{}}",null);

    assertU(commit());

    assertQ("Check the total number of docs", req("q","*:*"), "//result[@numFound=4]");

    //Tokens generated for "letter": "let" "ter" "letter" , maxTokenLen=3
    assertQ("Check the total number of docs", req("q","letter:let"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","letter:lett"), "//result[@numFound=0]");

    //Tokens generated for "whiteSpace in": "whi" "teS" "pac" "e" "in" "whiteSpace" , maxTokenLen=3
    assertQ("Check the total number of docs", req("q","whiteSpace:whi"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","whiteSpace:teS"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","whiteSpace:in"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","whiteSpace:white"), "//result[@numFound=0]");

    //Tokens generated for "unicode in": "uni" "cod" "e" "in" "unicode" , maxTokenLen=3
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace:uni"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace:cod"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace:e"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace:unico"), "//result[@numFound=0]");

    //Tokens generated for "keyword": "keyword" , maxTokenLen=3
    assertQ("Check the total number of docs", req("q","keyword:keyword"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","keyword:key"), "//result[@numFound=0]");

  }

  public void testSingleFieldSameAnalyzers() throws Exception {

    clearIndex();

    // using fields with definitions, same tokenizers both at index and query time.

    updateJ("{\"add\":{\"doc\": {\"id\":1,\"letter0\":\"letter\"}},\"commit\":{}}",null);
    updateJ("{\"add\":{\"doc\": {\"id\":3,\"whiteSpace0\":\"whiteSpace in\"}},\"commit\":{}}",null);
    updateJ("{\"add\":{\"doc\": {\"id\":4,\"unicodeWhiteSpace0\":\"unicode in\"}},\"commit\":{}}",null);
    updateJ("{\"add\":{\"doc\": {\"id\":5,\"keyword0\":\"keyword\"}},\"commit\":{}}",null);

    assertU(commit());

    assertQ("Check the total number of docs", req("q","*:*"), "//result[@numFound=4]");

    //Tokens generated for "letter": "let" "ter" "letter" , maxTokenLen=3
    // Anything that matches the first three letters should be found when maxLen=3
    assertQ("Check the total number of docs", req("q","letter0:l"), "//result[@numFound=0]");
    assertQ("Check the total number of docs", req("q","letter0:let"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","letter0:lett"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","letter0:letXYZ"), "//result[@numFound=1]");

    //Tokens generated for "whiteSpace in": "whi" "teS" "pac" "e" "in" "whiteSpace" , maxTokenLen=3
    // Anything that matches the first three letters should be found when maxLen=3
    assertQ("Check the total number of docs", req("q","whiteSpace0:h"), "//result[@numFound=0]");
    assertQ("Check the total number of docs", req("q","whiteSpace0:whi"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","whiteSpace0:teS"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","whiteSpace0:in"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","whiteSpace0:whiteZKY"), "//result[@numFound=1]");

    //Tokens generated for "unicode in": "uni" "cod" "e" "in" "unicode" , maxTokenLen=3
    // Anything that matches the first three letters should be found when maxLen=3
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace0:u"), "//result[@numFound=0]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace0:uni"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace0:cod"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace0:e"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","unicodeWhiteSpace0:unicoVBRT"), "//result[@numFound=1]");

    //Tokens generated for "keyword": "keyword" , maxTokenLen=3
    assertQ("Check the total number of docs", req("q","keyword0:keyword"), "//result[@numFound=1]");
    assertQ("Check the total number of docs", req("q","keyword0:key"), "//result[@numFound=0]");

  }
}
