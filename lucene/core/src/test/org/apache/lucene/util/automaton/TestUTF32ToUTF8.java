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


import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Util;

public class TestUTF32ToUTF8 extends LuceneTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  private static final int MAX_UNICODE = 0x10FFFF;

  private boolean matches(ByteRunAutomaton a, int code) {
    char[] chars = Character.toChars(code);
    byte[] b = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * chars.length];
    final int len = UnicodeUtil.UTF16toUTF8(chars, 0, chars.length, b);
    return a.run(b, 0, len);
  }

  private void testOne(Random r, ByteRunAutomaton a, int startCode, int endCode, int iters) {

    // Verify correct ints are accepted
    final int nonSurrogateCount;
    final boolean ovSurStart;
    if (endCode < UnicodeUtil.UNI_SUR_HIGH_START ||
        startCode > UnicodeUtil.UNI_SUR_LOW_END) {
      // no overlap w/ surrogates
      nonSurrogateCount = endCode - startCode + 1;
      ovSurStart = false;
    } else if (isSurrogate(startCode)) {
      // start of range overlaps surrogates
      nonSurrogateCount = endCode - startCode + 1 - (UnicodeUtil.UNI_SUR_LOW_END - startCode + 1);
      ovSurStart = false;
    } else if (isSurrogate(endCode)) {
      // end of range overlaps surrogates
      ovSurStart = true;
      nonSurrogateCount = endCode - startCode + 1 - (endCode - UnicodeUtil.UNI_SUR_HIGH_START + 1);
    } else {
      // range completely subsumes surrogates
      ovSurStart = true;
      nonSurrogateCount = endCode - startCode + 1 - (UnicodeUtil.UNI_SUR_LOW_END - UnicodeUtil.UNI_SUR_HIGH_START + 1);
    }

    assert nonSurrogateCount > 0;
        
    for(int iter=0;iter<iters;iter++) {
      // pick random code point in-range

      int code = startCode + r.nextInt(nonSurrogateCount);
      if (isSurrogate(code)) {
        if (ovSurStart) {
          code = UnicodeUtil.UNI_SUR_LOW_END + 1 + (code - UnicodeUtil.UNI_SUR_HIGH_START);
        } else {
          code = UnicodeUtil.UNI_SUR_LOW_END + 1 + (code - startCode);
        }
      }

      assert code >= startCode && code <= endCode: "code=" + code + " start=" + startCode + " end=" + endCode;
      assert !isSurrogate(code);

      assertTrue("DFA for range " + startCode + "-" + endCode + " failed to match code=" + code, 
                 matches(a, code));
    }

    // Verify invalid ints are not accepted
    final int invalidRange = MAX_UNICODE - (endCode - startCode + 1);
    if (invalidRange > 0) {
      for(int iter=0;iter<iters;iter++) {
        int x = TestUtil.nextInt(r, 0, invalidRange - 1);
        final int code;
        if (x >= startCode) {
          code = endCode + 1 + x - startCode;
        } else {
          code = x;
        }
        if ((code >= UnicodeUtil.UNI_SUR_HIGH_START && code <= UnicodeUtil.UNI_SUR_HIGH_END) |
            (code >= UnicodeUtil.UNI_SUR_LOW_START && code <= UnicodeUtil.UNI_SUR_LOW_END)) {
          iter--;
          continue;
        }
        assertFalse("DFA for range " + startCode + "-" + endCode + " matched invalid code=" + code,
                    matches(a, code));
                    
      }
    }
  }

  // Evenly picks random code point from the 4 "buckets"
  // (bucket = same #bytes when encoded to utf8)
  private int getCodeStart(Random r) {
    switch(r.nextInt(4)) {
    case 0:
      return TestUtil.nextInt(r, 0, 128);
    case 1:
      return TestUtil.nextInt(r, 128, 2048);
    case 2:
      return TestUtil.nextInt(r, 2048, 65536);
    default:
      return TestUtil.nextInt(r, 65536, 1 + MAX_UNICODE);
    }
  }

  private static boolean isSurrogate(int code) {
    return code >= UnicodeUtil.UNI_SUR_HIGH_START && code <= UnicodeUtil.UNI_SUR_LOW_END;
  }

  public void testRandomRanges() throws Exception {
    final Random r = random();
    int ITERS = atLeast(10);
    int ITERS_PER_DFA = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      int x1 = getCodeStart(r);
      int x2 = getCodeStart(r);
      final int startCode, endCode;

      if (x1 < x2) {
        startCode = x1;
        endCode = x2;
      } else {
        startCode = x2;
        endCode = x1;
      }

      if (isSurrogate(startCode) && isSurrogate(endCode)) {
        iter--;
        continue;
      }
      
      Automaton a = Automata.makeCharRange(startCode, endCode);
      testOne(r, new ByteRunAutomaton(a), startCode, endCode, ITERS_PER_DFA);
    }
  }

  public void testSpecialCase() {
    RegExp re = new RegExp(".?");
    Automaton automaton = re.toAutomaton();
    CharacterRunAutomaton cra = new CharacterRunAutomaton(automaton);
    ByteRunAutomaton bra = new ByteRunAutomaton(automaton);
    // make sure character dfa accepts empty string
    assertTrue(cra.isAccept(cra.getInitialState()));
    assertTrue(cra.run(""));
    assertTrue(cra.run(new char[0], 0, 0));

    // make sure byte dfa accepts empty string
    assertTrue(bra.isAccept(bra.getInitialState()));
    assertTrue(bra.run(new byte[0], 0, 0));
  }
  
  public void testSpecialCase2() throws Exception {
    RegExp re = new RegExp(".+\u0775");
    String input = "\ufadc\ufffd\ub80b\uda5a\udc68\uf234\u0056\uda5b\udcc1\ufffd\ufffd\u0775";
    Automaton automaton = re.toAutomaton();
    CharacterRunAutomaton cra = new CharacterRunAutomaton(automaton);
    ByteRunAutomaton bra = new ByteRunAutomaton(automaton);

    assertTrue(cra.run(input));
    
    byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
    assertTrue(bra.run(bytes, 0, bytes.length)); // this one fails!
  }
  
  public void testSpecialCase3() throws Exception {
    RegExp re = new RegExp("(\\鯺)*(.)*\\Ӕ");
    String input = "\u5cfd\ufffd\ub2f7\u0033\ue304\u51d7\u3692\udb50\udfb3\u0576\udae2\udc62\u0053\u0449\u04d4";
    Automaton automaton = re.toAutomaton();
    CharacterRunAutomaton cra = new CharacterRunAutomaton(automaton);
    ByteRunAutomaton bra = new ByteRunAutomaton(automaton);

    assertTrue(cra.run(input));
    
    byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
    assertTrue(bra.run(bytes, 0, bytes.length));
  }
  
  public void testRandomRegexes() throws Exception {
    int num = atLeast(250);
    for (int i = 0; i < num; i++) {
      assertAutomaton(new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE).toAutomaton());
    }
  }

  public void testSingleton() throws Exception {
    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      String s = TestUtil.randomRealisticUnicodeString(random());
      Automaton a = Automata.makeString(s);
      Automaton utf8 = new UTF32ToUTF8().convert(a);
      IntsRefBuilder ints = new IntsRefBuilder();
      Util.toIntsRef(new BytesRef(s), ints);
      Set<IntsRef> set = new HashSet<>();
      set.add(ints.get());
      assertEquals(set, TestOperations.getFiniteStrings(utf8));
    }
  }
  
  private void assertAutomaton(Automaton automaton) throws Exception {
    CharacterRunAutomaton cra = new CharacterRunAutomaton(automaton);
    ByteRunAutomaton bra = new ByteRunAutomaton(automaton);
    final AutomatonTestUtil.RandomAcceptedStrings ras = new AutomatonTestUtil.RandomAcceptedStrings(automaton);
    
    int num = atLeast(1000);
    for (int i = 0; i < num; i++) {
      final String string;
      if (random().nextBoolean()) {
        // likely not accepted
        string = TestUtil.randomUnicodeString(random());
      } else {
        // will be accepted
        int[] codepoints = ras.getRandomAcceptedString(random());
        try {
          string = UnicodeUtil.newString(codepoints, 0, codepoints.length);
        } catch (Exception e) {
          System.out.println(codepoints.length + " codepoints:");
          for(int j=0;j<codepoints.length;j++) {
            System.out.println("  " + Integer.toHexString(codepoints[j]));
          }
          throw e;
        }
      }
      byte bytes[] = string.getBytes(StandardCharsets.UTF_8);
      assertEquals(cra.run(string), bra.run(bytes, 0, bytes.length));
    }
  }
}
