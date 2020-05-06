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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Some simple regex tests, mostly converted from contrib's TestRegexQuery.
 */
public class TestRegexpQuery extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;
  private static final String FN = "field";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(newTextField(FN, "the quick brown fox jumps over the lazy ??? dog 493432 49344 [foo] 12.3", Field.Store.NO));
    writer.addDocument(doc);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  private Term newTerm(String value) {
    return new Term(FN, value);
  }
  
  private long regexQueryNrHits(String regex) throws IOException {
    RegexpQuery query = new RegexpQuery(newTerm(regex));
    return searcher.count(query);
  }
  
  public void testRegex1() throws IOException {
    assertEquals(1, regexQueryNrHits("q.[aeiou]c.*"));
  }
  
  public void testRegex2() throws IOException {
    assertEquals(0, regexQueryNrHits(".[aeiou]c.*"));
  }
  
  public void testRegex3() throws IOException {
    assertEquals(0, regexQueryNrHits("q.[aeiou]c"));
  }
  
  public void testNumericRange() throws IOException {
    assertEquals(1, regexQueryNrHits("<420000-600000>"));
    assertEquals(0, regexQueryNrHits("<493433-600000>"));
  }
  
  public void testCharacterClasses() throws IOException {
    assertEquals(0, regexQueryNrHits("\\d"));
    assertEquals(1, regexQueryNrHits("\\d*"));
    assertEquals(1, regexQueryNrHits("\\d{6}"));
    assertEquals(1, regexQueryNrHits("[a\\d]{6}"));
    assertEquals(1, regexQueryNrHits("\\d{2,7}"));
    assertEquals(0, regexQueryNrHits("\\d{4}"));
    assertEquals(0, regexQueryNrHits("\\dog"));
    assertEquals(1, regexQueryNrHits("493\\d32"));
    
    assertEquals(1, regexQueryNrHits("\\wox"));
    assertEquals(1, regexQueryNrHits("493\\w32"));
    assertEquals(1, regexQueryNrHits("\\?\\?\\?"));
    assertEquals(1, regexQueryNrHits("\\?\\W\\?"));
    assertEquals(1, regexQueryNrHits("\\?\\S\\?"));
    
    assertEquals(1, regexQueryNrHits("\\[foo\\]"));
    assertEquals(1, regexQueryNrHits("\\[\\w{3}\\]"));
    
    assertEquals(0, regexQueryNrHits("\\s.*")); // no matches because all whitespace stripped
    assertEquals(1, regexQueryNrHits("\\S*ck")); //matches quick
    assertEquals(1, regexQueryNrHits("[\\d\\.]{3,10}")); // matches 12.3
    assertEquals(1, regexQueryNrHits("\\d{1,3}(\\.(\\d{1,2}))+")); // matches 12.3
    
  }  
  
  public void testRegexComplement() throws IOException {
    assertEquals(1, regexQueryNrHits("4934~[3]"));
    // not the empty lang, i.e. match all docs
    assertEquals(1, regexQueryNrHits("~#"));
  }
  
  public void testCustomProvider() throws IOException {
    AutomatonProvider myProvider = new AutomatonProvider() {
      // automaton that matches quick or brown
      private Automaton quickBrownAutomaton = Operations.union(Arrays
          .asList(Automata.makeString("quick"),
          Automata.makeString("brown"),
          Automata.makeString("bob")));
      
      @Override
      public Automaton getAutomaton(String name) {
        if (name.equals("quickBrown")) return quickBrownAutomaton;
        else return null;
      }
    };
    RegexpQuery query = new RegexpQuery(newTerm("<quickBrown>"), RegExp.ALL,
      myProvider, DEFAULT_MAX_DETERMINIZED_STATES);
    assertEquals(1, searcher.search(query, 5).totalHits.value);
  }
  
  public void testCoreJavaParity() {
    // Generate random doc values and random regular expressions
    // and check for same matching behaviour as Java's Pattern class.
    for (int i = 0; i < 1000; i++) {
      checkRandomExpression(randomDocValue(1 + random().nextInt(30)));
    }        
  }

  static String randomDocValue(int minLength) {
    String charPalette = "AAAaaaBbbCccc123456 \t";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < minLength; i++) {
      sb.append(charPalette.charAt(randomInt(charPalette.length() - 1)));
    }
    return sb.toString();
  }

  private static int randomInt(int bound) {
    return bound == 0 ? 0 : random().nextInt(bound);
  }

  protected String checkRandomExpression(String docValue) {
    // Generate and test a random regular expression which should match the given docValue
    StringBuilder result = new StringBuilder();
    // Pick a part of the string to change
    int substitutionPoint = randomInt(docValue.length() - 1);
    int substitutionLength = 1 + randomInt(Math.min(10, docValue.length() - substitutionPoint));

    // Add any head to the result, unchanged
    if (substitutionPoint > 0) {
      result.append(docValue.substring(0, substitutionPoint));
    }

    // Modify the middle...
    String replacementPart = docValue.substring(substitutionPoint, substitutionPoint + substitutionLength);
    int mutation = random().nextInt(13);
    switch (mutation) {
      case 0:
        // OR with random alpha of same length
        result.append("(" + replacementPart + "|d" + randomDocValue(replacementPart.length()) + ")");
        break;
      case 1:
        // OR with non-existant value
        result.append("(" + replacementPart + "|doesnotexist)");
        break;
      case 2:
        // OR with another randomised regex (used to create nested levels of expression).
        result.append("(" + checkRandomExpression(replacementPart) + "|doesnotexist)");
        break;
      case 3:
        // Star-replace all ab sequences.
        result.append(replacementPart.replaceAll("ab", ".*"));
        break;
      case 4:
        // .-replace all b chars
        result.append(replacementPart.replaceAll("b", "."));
        break;
      case 5:
        // length-limited stars {1,2}
        result.append(".{1," + replacementPart.length() + "}");
        break;
      case 6:
        // replace all chars with .
        result.append(replacementPart.replaceAll(".", "."));
        break;
      case 7:
        // OR with uppercase chars eg [aA] (many of these sorts of expression in the wild..
        char[] chars = replacementPart.toCharArray();
        for (char c : chars) {
          result.append("[" + c + Character.toUpperCase(c) + "]");
        }
        break;
      case 8:
        // NOT a character - replace all b's with "not a"
        result.append(replacementPart.replaceAll("b", "[^a]"));
        break;
      case 9:
        // Make whole part repeatable 1 or more times
        result.append("(" + replacementPart + ")+");
        break;
      case 10:
        // Make whole part repeatable 0 or more times
        result.append("(" + replacementPart + ")?");
        break;
      case 11:
        // Make any digits replaced by character class
        result.append(replacementPart.replaceAll("\\d", "\\\\d"));
        break;
      case 12:
        // Make any whitespace chars replaced by not word class
        result.append(replacementPart.replaceAll("\\s", "\\\\W"));
        break;
      case 13:
        // Make any whitespace chars replace by whitespace class
        result.append(replacementPart.replaceAll("\\s", "\\\\s"));
        break;
      default:
        break;
    }
    // add any remaining tail, unchanged
    if (substitutionPoint + substitutionLength <= docValue.length() - 1) {
      result.append(docValue.substring(substitutionPoint + substitutionLength));
    }

    String regexPattern = result.toString();
    // Assert our randomly generated regex actually matches the provided raw input using java's expression matcher
    Pattern pattern = Pattern.compile(regexPattern);
    Matcher matcher = pattern.matcher(docValue);
    assertTrue("Java regex " + regexPattern + " did not match doc value " + docValue, matcher.matches());

    RegExp regex = new RegExp(regexPattern);
    Automaton automaton = regex.toAutomaton();
    ByteRunAutomaton bytesMatcher = new ByteRunAutomaton(automaton);
    BytesRef br = new BytesRef(docValue);
    assertTrue(
        "[" + regexPattern + "]should match [" + docValue + "]" + substitutionPoint + "-" + substitutionLength + "/"
            + docValue.length(),
        bytesMatcher.run(br.bytes, br.offset, br.length)
    );
    return regexPattern;
  }
  
  /**
   * Test a corner case for backtracking: In this case the term dictionary has
   * 493432 followed by 49344. When backtracking from 49343... to 4934, it's
   * necessary to test that 4934 itself is ok before trying to append more
   * characters.
   */
  public void testBacktracking() throws IOException {
    assertEquals(1, regexQueryNrHits("4934[314]"));
  }
}
