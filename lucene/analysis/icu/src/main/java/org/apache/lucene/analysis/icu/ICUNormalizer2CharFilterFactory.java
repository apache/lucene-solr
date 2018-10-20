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


import java.io.Reader;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;

import com.ibm.icu.text.FilteredNormalizer2;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.UnicodeSet;

/**
 * Factory for {@link ICUNormalizer2CharFilter}
 * <p>
 * Supports the following attributes:
 * <ul>
 *   <li>name: A <a href="http://unicode.org/reports/tr15/">Unicode Normalization Form</a>, 
 *       one of 'nfc','nfkc', 'nfkc_cf'. Default is nfkc_cf.
 *   <li>mode: Either 'compose' or 'decompose'. Default is compose. Use "decompose" with nfc
 *       or nfkc, to get nfd or nfkd, respectively.
 *   <li>filter: A {@link UnicodeSet} pattern. Codepoints outside the set are
 *       always left unchanged. Default is [] (the null set, no filtering).
 * </ul>
 * @see ICUNormalizer2CharFilter
 * @see Normalizer2
 * @see FilteredNormalizer2
 */
public class ICUNormalizer2CharFilterFactory extends CharFilterFactory implements MultiTermAwareComponent {
  private final Normalizer2 normalizer;

  /** Creates a new ICUNormalizer2CharFilterFactory */
  public ICUNormalizer2CharFilterFactory(Map<String,String> args) {
    super(args);
    String name = get(args, "name", "nfkc_cf");
    String mode = get(args, "mode", Arrays.asList("compose", "decompose"), "compose");
    Normalizer2 normalizer = Normalizer2.getInstance
        (null, name, "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE);
    
    String filter = get(args, "filter");
    if (filter != null) {
      UnicodeSet set = new UnicodeSet(filter);
      if (!set.isEmpty()) {
        set.freeze();
        normalizer = new FilteredNormalizer2(normalizer, set);
      }
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
    this.normalizer = normalizer;
  }

  @Override
  public Reader create(Reader input) {
    return new ICUNormalizer2CharFilter(input, normalizer);
  }

  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    return this;
  }
  
}
