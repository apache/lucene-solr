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

import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @since solr 1.4
 */
public class TestMultiWordSynonyms extends BaseTokenTestCase {

  @Test
  public void testMultiWordSynonyms() throws IOException {
    List<String> rules = new ArrayList<String>();
    rules.add("a b c,d");
    SynonymMap synMap = new SynonymMap(true);
    SynonymFilterFactory.parseRules(rules, synMap, "=>", ",", true, null);

    SynonymFilter ts = new SynonymFilter(new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader("a e")), synMap);
    // This fails because ["e","e"] is the value of the token stream
    assertTokenStreamContents(ts, new String[] { "a", "e" });
  }
}
