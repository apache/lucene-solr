package org.apache.solr.analysis;
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


import org.apache.solr.common.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 *
 *
 **/
public class TestStopFilterFactory extends BaseTokenTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new SolrResourceLoader(null, null);
    assertTrue("loader is null and it shouldn't be", loader != null);
    StopFilterFactory factory = new StopFilterFactory();
    Map<String, String> args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
    args.put("words", "stop-1.txt");
    args.put("ignoreCase", "true");
    factory.init(args);
    factory.inform(loader);
    Set<?> words = factory.getStopWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2, words.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory = new StopFilterFactory();
    args.put("words", "stop-1.txt, stop-2.txt");
    factory.init(args);
    factory.inform(loader);
    words = factory.getStopWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4, words.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);


  }
}
