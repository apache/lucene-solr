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
package org.apache.lucene.analysis.icu.segmentation;


import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.BreakIterator;

/**
 * An internal BreakIterator for multilingual text, following recommendations
 * from: UAX #29: Unicode Text Segmentation. (http://unicode.org/reports/tr29/)
 * <p>
 * See http://unicode.org/reports/tr29/#Tailoring for the motivation of this
 * design.
 * <p>
 * Text is first divided into script boundaries. The processing is then
 * delegated to the appropriate break iterator for that specific script.
 * <p>
 * This break iterator also allows you to retrieve the ISO 15924 script code
 * associated with a piece of text.
 * <p>
 * See also UAX #29, UTR #24
 * @lucene.experimental
 */
final class CompositeBreakIterator {
  private final ICUTokenizerConfig config;
  private final BreakIteratorWrapper wordBreakers[] = new BreakIteratorWrapper[1 + UCharacter.getIntPropertyMaxValue(UProperty.SCRIPT)];

  private BreakIteratorWrapper rbbi;
  private final ScriptIterator scriptIterator;

  private char text[];

  CompositeBreakIterator(ICUTokenizerConfig config) {
    this.config = config;
    this.scriptIterator = new ScriptIterator(config.combineCJ());
  }

  /**
   * Retrieve the next break position. If the RBBI range is exhausted within the
   * script boundary, examine the next script boundary.
   * 
   * @return the next break position or BreakIterator.DONE
   */
  int next() {
    int next = rbbi.next();
    while (next == BreakIterator.DONE && scriptIterator.next()) {
      rbbi = getBreakIterator(scriptIterator.getScriptCode());
      rbbi.setText(text, scriptIterator.getScriptStart(), 
          scriptIterator.getScriptLimit() - scriptIterator.getScriptStart());
      next = rbbi.next();
    }
    return (next == BreakIterator.DONE) ? BreakIterator.DONE : next
        + scriptIterator.getScriptStart();
  }

  /**
   * Retrieve the current break position.
   * 
   * @return the current break position or BreakIterator.DONE
   */
  int current() {
    final int current = rbbi.current();
    return (current == BreakIterator.DONE) ? BreakIterator.DONE : current
        + scriptIterator.getScriptStart();
  }

  /**
   * Retrieve the rule status code (token type) from the underlying break
   * iterator
   * 
   * @return rule status code (see RuleBasedBreakIterator constants)
   */
  int getRuleStatus() {
    return rbbi.getRuleStatus();
  }

  /**
   * Retrieve the UScript script code for the current token. This code can be
   * decoded with UScript into a name or ISO 15924 code.
   * 
   * @return UScript script code for the current token.
   */
  int getScriptCode() {
    return scriptIterator.getScriptCode();
  }

  /**
   * Set a new region of text to be examined by this iterator
   * 
   * @param text buffer of text
   * @param start offset into buffer
   * @param length maximum length to examine
   */
  void setText(final char text[], int start, int length) {
    this.text = text;
    scriptIterator.setText(text, start, length);
    if (scriptIterator.next()) {
      rbbi = getBreakIterator(scriptIterator.getScriptCode());
      rbbi.setText(text, scriptIterator.getScriptStart(), 
          scriptIterator.getScriptLimit() - scriptIterator.getScriptStart());
    } else {
      rbbi = getBreakIterator(UScript.COMMON);
      rbbi.setText(text, 0, 0);
    }
  }
  
  private BreakIteratorWrapper getBreakIterator(int scriptCode) {
    if (wordBreakers[scriptCode] == null)
      wordBreakers[scriptCode] = new BreakIteratorWrapper(config.getBreakIterator(scriptCode));
    return wordBreakers[scriptCode];
  }
}
