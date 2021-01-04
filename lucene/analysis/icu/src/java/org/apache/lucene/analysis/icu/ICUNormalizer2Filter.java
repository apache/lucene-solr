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
package org.apache.lucene.analysis.icu;

import com.ibm.icu.text.Normalizer;
import com.ibm.icu.text.Normalizer2;
import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Normalize token text with ICU's {@link com.ibm.icu.text.Normalizer2}
 *
 * <p>With this filter, you can normalize text in the following ways:
 *
 * <ul>
 *   <li>NFKC Normalization, Case Folding, and removing Ignorables (the default)
 *   <li>Using a standard Normalization mode (NFC, NFD, NFKC, NFKD)
 *   <li>Based on rules from a custom normalization mapping.
 * </ul>
 *
 * <p>If you use the defaults, this filter is a simple way to standardize Unicode text in a
 * language-independent way for search:
 *
 * <ul>
 *   <li>The case folding that it does can be seen as a replacement for LowerCaseFilter: For
 *       example, it handles cases such as the Greek sigma, so that "Μάϊος" and "ΜΆΪΟΣ" will match
 *       correctly.
 *   <li>The normalization will standardizes different forms of the same character in Unicode. For
 *       example, CJK full-width numbers will be standardized to their ASCII forms.
 *   <li>Ignorables such as Zero-Width Joiner and Variation Selectors are removed. These are
 *       typically modifier characters that affect display.
 * </ul>
 *
 * @see com.ibm.icu.text.Normalizer2
 * @see com.ibm.icu.text.FilteredNormalizer2
 */
public class ICUNormalizer2Filter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final Normalizer2 normalizer;
  private final StringBuilder buffer = new StringBuilder();

  /**
   * Create a new Normalizer2Filter that combines NFKC normalization, Case Folding, and removes
   * Default Ignorables (NFKC_Casefold)
   */
  public ICUNormalizer2Filter(TokenStream input) {
    this(input, Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));
  }

  /**
   * Create a new Normalizer2Filter with the specified Normalizer2
   *
   * @param input stream
   * @param normalizer normalizer to use
   */
  public ICUNormalizer2Filter(TokenStream input, Normalizer2 normalizer) {
    super(input);
    this.normalizer = normalizer;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (normalizer.quickCheck(termAtt) != Normalizer.YES) {
        buffer.setLength(0);
        normalizer.normalize(termAtt, buffer);
        termAtt.setEmpty().append(buffer);
      }
      return true;
    } else {
      return false;
    }
  }
}
