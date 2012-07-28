package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;

import java.util.Map;
import java.util.HashMap;

/**
 *
 *
 **/
public class TestKeepFilterFactory extends BaseTokenStreamTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    assertTrue("loader is null and it shouldn't be", loader != null);
    KeepWordFilterFactory factory = new KeepWordFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put("words", "keep-1.txt");
    args.put("ignoreCase", "true");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    CharArraySet words = factory.getWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2, words.size() == 2);


    factory = new KeepWordFilterFactory();
    args.put("words", "keep-1.txt, keep-2.txt");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    words = factory.getWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4, words.size() == 4);



  }
}