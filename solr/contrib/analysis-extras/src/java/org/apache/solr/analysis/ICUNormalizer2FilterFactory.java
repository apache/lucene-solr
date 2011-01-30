package org.apache.solr.analysis;

/**
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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUNormalizer2Filter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import com.ibm.icu.text.FilteredNormalizer2;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.UnicodeSet;

/**
 * Factory for {@link ICUNormalizer2Filter}
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
 * @see ICUNormalizer2Filter
 * @see Normalizer2
 * @see FilteredNormalizer2
 */
public class ICUNormalizer2FilterFactory extends BaseTokenFilterFactory {
  private Normalizer2 normalizer;

  // TODO: support custom normalization
  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    String name = args.get("name");
    if (name == null)
      name = "nfkc_cf";
    String mode = args.get("mode");
    if (mode == null)
      mode = "compose";
    
    if (mode.equals("compose"))
      normalizer = Normalizer2.getInstance(null, name, Normalizer2.Mode.COMPOSE);
    else if (mode.equals("decompose"))
      normalizer = Normalizer2.getInstance(null, name, Normalizer2.Mode.DECOMPOSE);
    else 
      throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid mode: " + mode);
    
    String filter = args.get("filter");
    if (filter != null) {
      UnicodeSet set = new UnicodeSet(filter);
      if (!set.isEmpty()) {
        set.freeze();
        normalizer = new FilteredNormalizer2(normalizer, set);
      }
    }
  }
  
  public TokenStream create(TokenStream input) {
    return new ICUNormalizer2Filter(input, normalizer);
  }
}
