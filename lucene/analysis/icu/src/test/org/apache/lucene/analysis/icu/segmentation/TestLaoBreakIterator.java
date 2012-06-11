package org.apache.lucene.analysis.icu.segmentation;

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

import java.io.InputStream;

import org.apache.lucene.util.LuceneTestCase;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import com.ibm.icu.text.UTF16;

/**
 * Tests LaoBreakIterator and its RBBI rules
 */
public class TestLaoBreakIterator extends LuceneTestCase {
  private BreakIterator wordIterator;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    InputStream is = getClass().getResourceAsStream("Lao.brk");
    wordIterator = new LaoBreakIterator(RuleBasedBreakIterator.getInstanceFromCompiledRules(is));
    is.close();
  }
  
  private void assertBreaksTo(BreakIterator iterator, String sourceText, String tokens[]) {
    char text[] = sourceText.toCharArray();
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText(text, 0, text.length);
    iterator.setText(ci);
    
    for (int i = 0; i < tokens.length; i++) {
      int start, end;
      do {
        start = iterator.current();
        end = iterator.next();
      } while (end != BreakIterator.DONE && !isWord(text, start, end));
      assertTrue(start != BreakIterator.DONE);
      assertTrue(end != BreakIterator.DONE);
      assertEquals(tokens[i], new String(text, start, end - start));
    }
    
    assertTrue(iterator.next() == BreakIterator.DONE);
  }
  
  protected boolean isWord(char text[], int start, int end) {
    int codepoint;
    for (int i = start; i < end; i += UTF16.getCharCount(codepoint)) {
      codepoint = UTF16.charAt(text, 0, end, start);

      if (UCharacter.isLetterOrDigit(codepoint))
        return true;
      }

    return false;
  }
  
  public void testBasicUsage() throws Exception {
    assertBreaksTo(wordIterator, "ກວ່າດອກ", new String[] { "ກວ່າ", "ດອກ" });
    assertBreaksTo(wordIterator, "ຜູ້​ເຂົ້າ", new String[] { "ຜູ້", "ເຂົ້າ" });
    assertBreaksTo(wordIterator, "", new String[] {});
    assertBreaksTo(wordIterator, "ສະບາຍດີ", new String[] { "ສະ", "ບາຍ", "ດີ" });
  }
  
  public void testNumerics() throws Exception {
    assertBreaksTo(wordIterator, "໐໑໒໓", new String[] { "໐໑໒໓" });
    assertBreaksTo(wordIterator, "໐໑໒໓.໕໖", new String[] { "໐໑໒໓.໕໖" });
  }
 
  public void testTextAndNumerics() throws Exception {
    assertBreaksTo(wordIterator, "ກວ່າດອກ໐໑໒໓", new String[] { "ກວ່າ", "ດອກ", "໐໑໒໓" });
  }
}
