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
package org.apache.lucene.util.automaton;


import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegExp extends LuceneTestCase {

  /**
   * Simple smoke test for regular expression.
   */
  public void testSmoke() {
    RegExp r = new RegExp("a(b+|c+)d");
    Automaton a = r.toAutomaton();
    assertTrue(a.isDeterministic());
    CharacterRunAutomaton run = new CharacterRunAutomaton(a);
    assertTrue(run.run("abbbbbd"));
    assertTrue(run.run("acd"));
    assertFalse(run.run("ad"));
  }

  /**
   * Compiles a regular expression that is prohibitively expensive to
   * determinize and expexts to catch an exception for it.
   */
  public void testDeterminizeTooManyStates() {
    // LUCENE-6046
    String source = "[ac]*a[ac]{50,200}";
    TooComplexToDeterminizeException expected = expectThrows(TooComplexToDeterminizeException.class, () -> {
      new RegExp(source).toAutomaton();
    });
    assertTrue(expected.getMessage().contains(source));
  }

  public void testSerializeTooManyStatesToRepeat() throws Exception {
    String source = "a{50001}";
    TooComplexToDeterminizeException expected = expectThrows(TooComplexToDeterminizeException.class, () -> {
      new RegExp(source).toAutomaton(50000);
    });
    assertTrue(expected.getMessage().contains(source));
  }

  // LUCENE-6713
  public void testSerializeTooManyStatesToDeterminizeExc() throws Exception {
    // LUCENE-6046
    String source = "[ac]*a[ac]{50,200}";
    TooComplexToDeterminizeException expected = expectThrows(TooComplexToDeterminizeException.class, () -> {
      new RegExp(source).toAutomaton();
    });
    assertTrue(expected.getMessage().contains(source));
  }

  // LUCENE-6046
  public void testRepeatWithEmptyString() throws Exception {
    Automaton a = new RegExp("[^y]*{1,2}").toAutomaton(1000);
    // paranoia:
    assertTrue(a.toString().length() > 0);
  }

  public void testRepeatWithEmptyLanguage() throws Exception {
    Automaton a = new RegExp("#*").toAutomaton(1000);
    // paranoia:
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#+").toAutomaton(1000);
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#{2,10}").toAutomaton(1000);
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#?").toAutomaton(1000);
    assertTrue(a.toString().length() > 0);
  }
  
  boolean caseSensitiveQuery = true;
  
  public void testCoreJavaParity() {
    // Generate random doc values and random regular expressions
    // and check for same matching behaviour as Java's Pattern class.
    for (int i = 0; i < 1000; i++) {
      caseSensitiveQuery = true;      
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
    int mutation = random().nextInt(12);
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
        // Switch case of characters
        StringBuilder switchedCase = new StringBuilder();
        replacementPart.codePoints().forEach(
            p -> {
              int switchedP = p;
              if (Character.isLowerCase(p)) {
                switchedP = Character.toUpperCase(p);
              } else {
                switchedP = Character.toLowerCase(p);                
              }
              switchedCase.appendCodePoint(switchedP);
              if (p != switchedP) {
                caseSensitiveQuery = false;
              }
            }
        );        
        result.append(switchedCase.toString());
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
    Pattern pattern = caseSensitiveQuery ? Pattern.compile(regexPattern): 
                                           Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE); 
                                             ;
    Matcher matcher = pattern.matcher(docValue);
    assertTrue("Java regex " + regexPattern + " did not match doc value " + docValue, matcher.matches());

    int matchFlags = caseSensitiveQuery ? 0 : RegExp.ASCII_CASE_INSENSITIVE;
    RegExp regex =  new RegExp(regexPattern, RegExp.ALL, matchFlags);
    Automaton automaton = regex.toAutomaton();
    ByteRunAutomaton bytesMatcher = new ByteRunAutomaton(automaton);
    BytesRef br = new BytesRef(docValue);
    assertTrue(
        "[" + regexPattern + "]should match [" + docValue + "]" + substitutionPoint + "-" + substitutionLength + "/"
            + docValue.length(),
        bytesMatcher.run(br.bytes, br.offset, br.length)
    );
    if (caseSensitiveQuery == false) {
      RegExp caseSensitiveRegex = new RegExp(regexPattern);
      Automaton csAutomaton = caseSensitiveRegex.toAutomaton();
      ByteRunAutomaton csBytesMatcher = new ByteRunAutomaton(csAutomaton);
      assertFalse(
          "[" + regexPattern + "] with case sensitive setting should not match [" + docValue + "]", 
          csBytesMatcher.run(br.bytes, br.offset, br.length)
      );
      
    }
    return regexPattern;
  }
  
}
