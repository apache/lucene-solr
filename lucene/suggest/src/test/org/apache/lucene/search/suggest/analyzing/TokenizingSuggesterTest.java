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

package org.apache.lucene.search.suggest.analyzing;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

public class TokenizingSuggesterTest extends LuceneTestCase {

  public void testBasic() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    TokenizingSuggester suggester = new TokenizingSuggester(newDirectory(), a, a, a,3, false);
    suggester.build(new InputArrayIterator(keys));

    List<Lookup.LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("earned", results.get(0).key);
    assertEquals("<b>ear</b>ned", results.get(0).highlightKey);
    assertEquals(1, results.get(0).value);
    assertEquals("foobaz", results.get(0).payload.utf8ToString());

    assertEquals("ear", results.get(1).key);
    assertEquals("<b>ear</b>", results.get(1).highlightKey);
    assertEquals(1, results.get(1).value);
    assertEquals(new BytesRef("foobar"), results.get(1).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("ear ", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("ear", results.get(0).key);
    assertEquals("<b>ear</b>", results.get(0).highlightKey);
    assertEquals(1, results.get(0).value);
    assertEquals(new BytesRef("foobar"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("pen", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("penny", results.get(0).key);
    assertEquals("<b>pen</b>ny", results.get(0).highlightKey);
    assertEquals(2, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("p", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("penny", results.get(0).key);
    assertEquals("<b>p</b>enny", results.get(0).highlightKey);
    assertEquals(2, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("money penny", random()), 10, false, true);
    assertEquals(1, results.size());
    assertEquals("penny", results.get(0).key);
    assertEquals("<b>penny</b>", results.get(0).highlightKey);
    assertEquals(2, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("penny ea", random()), 10, false, true);
    assertEquals(3, results.size());
    assertEquals("earned", results.get(0).key);
    assertEquals("ear", results.get(1).key);
    assertEquals("penny", results.get(2).key);

    suggester.close();
    a.close();
  }

}
