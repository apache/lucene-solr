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
package org.apache.solr.spelling;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.Test;

public class SpellPossibilityIteratorTest extends SolrTestCaseJ4 {
  private static final Token TOKEN_AYE = new Token("AYE", 0, 3);
  private static final Token TOKEN_BEE = new Token("BEE", 4, 7);
  private static final Token TOKEN_AYE_BEE = new Token("AYE BEE", 0, 7);
  private static final Token TOKEN_CEE = new Token("CEE", 8, 11);

  private LinkedHashMap<String, Integer> AYE;
  private LinkedHashMap<String, Integer> BEE;
  private LinkedHashMap<String, Integer> AYE_BEE;
  private LinkedHashMap<String, Integer> CEE;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    AYE = new LinkedHashMap<>();
    AYE.put("I", 0);
    AYE.put("II", 0);
    AYE.put("III", 0);
    AYE.put("IV", 0);
    AYE.put("V", 0);
    AYE.put("VI", 0);
    AYE.put("VII", 0);
    AYE.put("VIII", 0);

    BEE = new LinkedHashMap<>();
    BEE.put("alpha", 0);
    BEE.put("beta", 0);
    BEE.put("gamma", 0);
    BEE.put("delta", 0);
    BEE.put("epsilon", 0);
    BEE.put("zeta", 0);
    BEE.put("eta", 0);
    BEE.put("theta", 0);
    BEE.put("iota", 0);

    AYE_BEE = new LinkedHashMap<>();
    AYE_BEE.put("one-alpha", 0);
    AYE_BEE.put("two-beta", 0);
    AYE_BEE.put("three-gamma", 0);
    AYE_BEE.put("four-delta", 0);
    AYE_BEE.put("five-epsilon", 0);
    AYE_BEE.put("six-zeta", 0);
    AYE_BEE.put("seven-eta", 0);
    AYE_BEE.put("eight-theta", 0);
    AYE_BEE.put("nine-iota", 0);


    CEE = new LinkedHashMap<>();
    CEE.put("one", 0);
    CEE.put("two", 0);
    CEE.put("three", 0);
    CEE.put("four", 0);
    CEE.put("five", 0);
    CEE.put("six", 0);
    CEE.put("seven", 0);
    CEE.put("eight", 0);
    CEE.put("nine", 0);
    CEE.put("ten", 0);
  }

  @Test
  public void testScalability() throws Exception {
    Map<Token, LinkedHashMap<String, Integer>> lotsaSuggestions = new LinkedHashMap<>();
    lotsaSuggestions.put(TOKEN_AYE , AYE);
    lotsaSuggestions.put(TOKEN_BEE , BEE);
    lotsaSuggestions.put(TOKEN_CEE , CEE);
    
    lotsaSuggestions.put(new Token("AYE1", 0, 3),  AYE);
    lotsaSuggestions.put(new Token("BEE1", 4, 7),  BEE);
    lotsaSuggestions.put(new Token("CEE1", 8, 11), CEE);
    
    lotsaSuggestions.put(new Token("AYE2", 0, 3),  AYE);
    lotsaSuggestions.put(new Token("BEE2", 4, 7),  BEE);
    lotsaSuggestions.put(new Token("CEE2", 8, 11), CEE);
    
    lotsaSuggestions.put(new Token("AYE3", 0, 3),  AYE);
    lotsaSuggestions.put(new Token("BEE3", 4, 7),  BEE);
    lotsaSuggestions.put(new Token("CEE3", 8, 11), CEE);
    
    lotsaSuggestions.put(new Token("AYE4", 0, 3),  AYE);
    lotsaSuggestions.put(new Token("BEE4", 4, 7),  BEE);
    lotsaSuggestions.put(new Token("CEE4", 8, 11), CEE);
    
    PossibilityIterator iter = new PossibilityIterator(lotsaSuggestions, 1000, 10000, false);
    int count = 0;
    while (iter.hasNext()) {
      PossibilityIterator.RankedSpellPossibility rsp = iter.next();
      count++;
    }
    assertTrue(count==1000);

    lotsaSuggestions.put(new Token("AYE_BEE1", 0, 7), AYE_BEE);
    lotsaSuggestions.put(new Token("AYE_BEE2", 0, 7), AYE_BEE);
    lotsaSuggestions.put(new Token("AYE_BEE3", 0, 7), AYE_BEE);
    lotsaSuggestions.put(new Token("AYE_BEE4", 0, 7), AYE_BEE);
    iter = new PossibilityIterator(lotsaSuggestions, 1000, 10000, true);
    count = 0;
    while (iter.hasNext()) {      
      PossibilityIterator.RankedSpellPossibility rsp = iter.next();
      count++;
    }
    assertTrue(count<100);
  }

  @Test
  public void testSpellPossibilityIterator() throws Exception {
    Map<Token, LinkedHashMap<String, Integer>> suggestions = new LinkedHashMap<>();
    suggestions.put(TOKEN_AYE , AYE);
    suggestions.put(TOKEN_BEE , BEE);
    suggestions.put(TOKEN_CEE , CEE);
    
    PossibilityIterator iter = new PossibilityIterator(suggestions, 1000, 10000, false);
    int count = 0;
    while (iter.hasNext()) {

      PossibilityIterator.RankedSpellPossibility rsp = iter.next();
      if(count==0) {
        assertTrue("I".equals(rsp.corrections.get(0).getCorrection()));
        assertTrue("alpha".equals(rsp.corrections.get(1).getCorrection()));
        assertTrue("one".equals(rsp.corrections.get(2).getCorrection()));
      }
      count++;
    }
    assertTrue(("Three maps (8*9*10) should return 720 iterations but instead returned " + count), count == 720);

    suggestions.remove(TOKEN_CEE);
    iter = new PossibilityIterator(suggestions, 100, 10000, false);
    count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertTrue(("Two maps (8*9) should return 72 iterations but instead returned " + count), count == 72);

    suggestions.remove(TOKEN_BEE);
    iter = new PossibilityIterator(suggestions, 5, 10000, false);
    count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertTrue(("We requested 5 suggestions but got " + count), count == 5);

    suggestions.remove(TOKEN_AYE);
    iter = new PossibilityIterator(suggestions, Integer.MAX_VALUE, 10000, false);
    count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertTrue(("No maps should return 0 iterations but instead returned " + count), count == 0);

  }

  @Test
  public void testOverlappingTokens() throws Exception {
    Map<Token, LinkedHashMap<String, Integer>> overlappingSuggestions = new LinkedHashMap<>();
    overlappingSuggestions.put(TOKEN_AYE, AYE);
    overlappingSuggestions.put(TOKEN_BEE, BEE);
    overlappingSuggestions.put(TOKEN_AYE_BEE, AYE_BEE);
    overlappingSuggestions.put(TOKEN_CEE, CEE);
    
    PossibilityIterator iter = new PossibilityIterator(overlappingSuggestions, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
    int aCount = 0;
    int abCount = 0;
    Set<PossibilityIterator.RankedSpellPossibility> dupChecker = new HashSet<>();
    while (iter.hasNext()) {
      PossibilityIterator.RankedSpellPossibility rsp = iter.next();
      Token a = null;
      Token b = null;
      Token ab = null;
      Token c = null;
      for(SpellCheckCorrection scc : rsp.corrections) {
        if(scc.getOriginal().equals(TOKEN_AYE)) {
          a = scc.getOriginal();
        } else if(scc.getOriginal().equals(TOKEN_BEE)) {
          b = scc.getOriginal();
        } else if(scc.getOriginal().equals(TOKEN_AYE_BEE)) {
          ab = scc.getOriginal();
        } else if(scc.getOriginal().equals(TOKEN_CEE)) {
          c = scc.getOriginal();
        }       
        if(ab!=null) {
          abCount++;
        } else {
          aCount++;
        }       
      }
      assertTrue(c != null);
      assertTrue(ab != null || (a!=null && b!=null));
      assertTrue(ab == null || (a==null && b==null));
      assertTrue(dupChecker.add(rsp));
    }
    assertTrue(aCount==2160);
    assertTrue(abCount==180);
  }
}
