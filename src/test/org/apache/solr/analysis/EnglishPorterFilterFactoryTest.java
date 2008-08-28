package org.apache.solr.analysis;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;

public class EnglishPorterFilterFactoryTest extends BaseTokenTestCase {

  public void test() throws IOException {
    EnglishStemmer stemmer = new EnglishStemmer();
    String[] test = {"The", "fledgling", "banks", "were", "counting", "on", "a", "big", "boom", "in", "banking"};
    StringBuilder gold = new StringBuilder();
    for (int i = 0; i < test.length; i++) {
      stemmer.setCurrent(test[i]);
      stemmer.stem();
      gold.append(stemmer.getCurrent()).append(' ');
    }

    EnglishPorterFilterFactory factory = new EnglishPorterFilterFactory();
    Map<String, String> args = new HashMap<String, String>();

    factory.init(args);
    factory.inform(new LinesMockSolrResourceLoader(new ArrayList<String>()));
    String out = tsToString(factory.create(new IterTokenStream(test)));
    assertEquals(gold.toString().trim(), out);
  }

  public void testProtected() throws Exception {
    EnglishStemmer stemmer = new EnglishStemmer();
    String[] test = {"The", "fledgling", "banks", "were", "counting", "on", "a", "big", "boom", "in", "banking"};
    StringBuilder gold = new StringBuilder();
    for (int i = 0; i < test.length; i++) {
      if (test[i].equals("fledgling") == false && test[i].equals("banks") == false) {
        stemmer.setCurrent(test[i]);
        stemmer.stem();
        gold.append(stemmer.getCurrent()).append(' ');
      } else {
        gold.append(test[i]).append(' ');
      }
    }

    EnglishPorterFilterFactory factory = new EnglishPorterFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put(EnglishPorterFilterFactory.PROTECTED_TOKENS, "who-cares.txt");
    factory.init(args);
    List<String> lines = new ArrayList<String>();
    Collections.addAll(lines, "banks", "fledgling");
    factory.inform(new LinesMockSolrResourceLoader(lines));
    String out = tsToString(factory.create(new IterTokenStream(test)));
    assertEquals(gold.toString().trim(), out);
  }

  class LinesMockSolrResourceLoader implements ResourceLoader {
    List<String> lines;

    LinesMockSolrResourceLoader(List<String> lines) {
      this.lines = lines;
    }

    public List<String> getLines(String resource) throws IOException {
      return lines;
    }

    public Object newInstance(String cname, String... subpackages) {
      return null;
    }

    public InputStream openResource(String resource) throws IOException {
      return null;
    }
  }
}

