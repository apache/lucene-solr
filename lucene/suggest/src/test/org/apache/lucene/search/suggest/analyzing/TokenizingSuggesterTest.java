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
    assertEquals("penny", results.get(0).key);
    assertEquals("earned", results.get(1).key);
    assertEquals("ear", results.get(2).key);

    suggester.close();
    a.close();
  }

  public void testVT() throws Exception {
    Input keys[] = new Input[] {
        new Input("On 4 June 2001, the conclusions Court approved the request from the Public Prosecutor to produce the exhibits of weapons and ammunitions seized in this case. The Panel also decided about an oral motion presented by the Defense about rules of evidence. By ruling on those motions, the Court decided to: (a) admit and deem relevant the testimonies listed by the Public Prosecutor in the folders Case A and Case B, as modified by the submission of 15 May 2001; (b) to admit and deem relevant the two experts reports about crimes against humanity and the one about the crime scene; (c) to admit the original photographs and video and maps; (d) to admit and deem relevant the exhibits consisting of weapons and ammunition; and (e) remind the Public Prosecutor about his duty to make the evidence accessible to the Defense",
            8, new BytesRef("foobar")),
        new Input("After that, the Court moved to the statement of the accuseds concerning the count 6 (murder of Alfredo Araújo and Kalistu Rodrigues). The accused Joni Marques once again remarked that he had no further statement to make, after the one he made concerning the same charge on 10 July 2001. The accused Paulo da Costa admitted that he shot Alfredo Araújo because Joni Marques offered him a gun to shoot the victim, but he would not plead guilty to the charge because he was ordered to shoot. Regarding the killing of Kalistu Rodrigues, Paulo stated that the main perpetrator was Joni Marques. The accused Alarico Fernandes also confirmed that he understood the nature of the charge against him. He then stated that he was told to go with the others from Com to Lautem to get vegetables. When they arrived at the scene of the incident, Alfredo Araújo and Kalistu Rodrigues were killed. But he did not participate; he had no bad plan to kill anybody; he had only gone to Ira-Ara to look for vegetables. The Court decided that there was no admission of guilt and continued with the cross-examination of the accused. The Court and the Prosecutor questioned the accused Joni Marques.",
            10, new BytesRef("foobaz")),
        new Input("2 October 2001 – the Court connection decided that the Prosecution had to prove the connection of each exhibit to the alleged crimes committed and to show what kind of evidence was coming before the Court. “Such presentation has to be done immediately before the Court closes the presentation and hearing of evidence. On the other hand, the Defence has the right to contradict any allegation of connection between the exhibit and the accused or the crime allegedly committed. During their closing statements the parties make their conclusions based on the evidence already discussed before the Court and contradicted by the opposite party. The Court will then draw its own conclusions and may consider them as evidence in the case if it deems them relevant and as having probative value with regard to the issues in dispute (Sect. 34.1 of UR-30/2000). Therefore, the Court orders the Public Prosecutor to show the connection of each exhibit with the charges and the conduct of the accuseds, one by one. As soon as the presentation is completed, the Defence will be given an opportunity to respond immediately to the submission of the Public Prosecutor\".",
            12, new BytesRef("foobark")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, false);
    TokenizingSuggester suggester = new TokenizingSuggester(newDirectory(), a, a, a,3, false);
    suggester.build(new InputArrayIterator(keys));

    List<Lookup.LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("co", random()), 10, true, true);
    assertEquals(10, results.size());
    assertEquals("connection", results.get(0).key);
    assertEquals(4, results.get(0).value);
    assertEquals("conclusions", results.get(1).key);
    assertEquals(3, results.get(1).value);
    assertEquals("committed", results.get(2).key);
    assertEquals(2, results.get(2).value);

    suggester.close();
    a.close();
  }

}
