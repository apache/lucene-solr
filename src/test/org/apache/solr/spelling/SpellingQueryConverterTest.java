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

package org.apache.solr.spelling;

import java.util.Collection;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.AbstractSolrTestCase;


/**
 *
 * @since solr 1.3
 **/
public class SpellingQueryConverterTest extends AbstractSolrTestCase {

  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }


  public void test() throws Exception {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    Collection<Token> tokens = converter.convert("field:foo");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not: " + 1, tokens.size() == 1);
  }


}
