package org.apache.lucene.analysis.phonetic;

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

import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.language.bm.Languages.LanguageSet;
import org.apache.commons.codec.language.bm.NameType;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.commons.codec.language.bm.RuleType;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/** 
 * Factory for {@link BeiderMorseFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_bm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.BeiderMorseFilterFactory"
 *        nameType="GENERIC" ruleType="APPROX" 
 *        concat="true" languageSet="auto"
 *     &lt;/filter&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class BeiderMorseFilterFactory extends TokenFilterFactory {
  private final PhoneticEngine engine;
  private final LanguageSet languageSet;
  
  /** Creates a new BeiderMorseFilterFactory */
  public BeiderMorseFilterFactory(Map<String,String> args) {
    super(args);
    // PhoneticEngine = NameType + RuleType + concat
    // we use common-codec's defaults: GENERIC + APPROX + true
    NameType nameType = NameType.valueOf(get(args, "nameType", NameType.GENERIC.toString()));
    RuleType ruleType = RuleType.valueOf(get(args, "ruleType", RuleType.APPROX.toString()));
    
    boolean concat = getBoolean(args, "concat", true);
    engine = new PhoneticEngine(nameType, ruleType, concat);
    
    // LanguageSet: defaults to automagic, otherwise a comma-separated list.
    Set<String> langs = getSet(args, "languageSet");
    languageSet = (null == langs || (1 == langs.size() && langs.contains("auto"))) ? null : LanguageSet.from(langs);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new BeiderMorseFilter(input, engine, languageSet);
  }
}
