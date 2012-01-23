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


import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Testcase for {@link TypeTokenFilterFactory}
 */
public class TestTypeTokenFilterFactory extends BaseTokenTestCase {

  @Test
  public void testInform() throws Exception {
    ResourceLoader loader = new SolrResourceLoader(null, null);
    TypeTokenFilterFactory factory = new TypeTokenFilterFactory();
    Map<String, String> args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
    args.put("types", "stoptypes-1.txt");
    args.put("enablePositionIncrements", "true");
    factory.init(args);
    factory.inform(loader);
    Set<String> types = factory.getStopTypes();
    assertTrue("types is null and it shouldn't be", types != null);
    assertTrue("types Size: " + types.size() + " is not: " + 2, types.size() == 2);
    assertTrue("enablePositionIncrements was set to true but not correctly parsed", factory.isEnablePositionIncrements());

    factory = new TypeTokenFilterFactory();
    args.put("types", "stoptypes-1.txt, stoptypes-2.txt");
    args.put("enablePositionIncrements", "false");
    factory.init(args);
    factory.inform(loader);
    types = factory.getStopTypes();
    assertTrue("types is null and it shouldn't be", types != null);
    assertTrue("types Size: " + types.size() + " is not: " + 4, types.size() == 4);
    assertTrue("enablePositionIncrements was set to false but not correctly parsed", !factory.isEnablePositionIncrements());
  }

  @Test
  public void testCreation() throws Exception {
    TypeTokenFilterFactory typeTokenFilterFactory = new TypeTokenFilterFactory();
    Map<String, String> args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
    args.put("types", "stoptypes-1.txt, stoptypes-2.txt");
    args.put("enablePositionIncrements", "false");
    typeTokenFilterFactory.init(args);
    NumericTokenStream input = new NumericTokenStream();
    input.setIntValue(123);
    typeTokenFilterFactory.create(input);
  }

  @Test
  public void testMissingTypesParameter() throws Exception {
    try {
      TypeTokenFilterFactory typeTokenFilterFactory = new TypeTokenFilterFactory();
      Map<String, String> args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
      args.put("enablePositionIncrements", "false");
      typeTokenFilterFactory.init(args);
      typeTokenFilterFactory.inform(new SolrResourceLoader(null, null));
      fail("not supplying 'types' parameter should cause a SolrException");
    } catch (SolrException e) {
      // everything ok
    }
  }

}
