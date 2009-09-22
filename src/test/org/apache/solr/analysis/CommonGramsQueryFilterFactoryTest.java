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
import org.apache.solr.common.ResourceLoader;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 * Tests pretty much copied from StopFilterFactoryTest We use the test files
 * used by the StopFilterFactoryTest TODO: consider creating separate test files
 * so this won't break if stop filter test files change
 **/
public class CommonGramsQueryFilterFactoryTest extends AbstractSolrTestCase {
  public String getSchemaFile() {
    return "schema-stop-keep.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void testInform() throws Exception {
    ResourceLoader loader = solrConfig.getResourceLoader();
    assertTrue("loader is null and it shouldn't be", loader != null);
    CommonGramsQueryFilterFactory factory = new CommonGramsQueryFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put("words", "stop-1.txt");
    args.put("ignoreCase", "true");
    factory.init(args);
    factory.inform(loader);
    Set words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2,
        words.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory
        .isIgnoreCase() == true);

    factory = new CommonGramsQueryFilterFactory();
    args.put("words", "stop-1.txt, stop-2.txt");
    factory.init(args);
    factory.inform(loader);
    words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4,
        words.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory
        .isIgnoreCase() == true);

  }
}
