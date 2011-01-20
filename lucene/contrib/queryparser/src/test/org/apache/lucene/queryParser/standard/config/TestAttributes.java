package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

public class TestAttributes extends LuceneTestCase {

  // this checks using reflection API if the defaults are correct
  public void testAttributes() {
    _TestUtil.assertAttributeReflection(new AllowLeadingWildcardAttributeImpl(),
      Collections.singletonMap(AllowLeadingWildcardAttribute.class.getName()+"#allowLeadingWildcard", false));
    _TestUtil.assertAttributeReflection(new AnalyzerAttributeImpl(),
      Collections.singletonMap(AnalyzerAttribute.class.getName()+"#analyzer", null));
    _TestUtil.assertAttributeReflection(new BoostAttributeImpl(),
      Collections.singletonMap(BoostAttribute.class.getName()+"#boost", 1.0f));
    _TestUtil.assertAttributeReflection(new DateResolutionAttributeImpl(),
      Collections.singletonMap(DateResolutionAttribute.class.getName()+"#dateResolution", null));
    _TestUtil.assertAttributeReflection(new DefaultOperatorAttributeImpl(),
      Collections.singletonMap(DefaultOperatorAttribute.class.getName()+"#operator", DefaultOperatorAttribute.Operator.OR));
    _TestUtil.assertAttributeReflection(new DefaultPhraseSlopAttributeImpl(),
      Collections.singletonMap(DefaultPhraseSlopAttribute.class.getName()+"#defaultPhraseSlop", 0));
    _TestUtil.assertAttributeReflection(new FieldBoostMapAttributeImpl(),
      Collections.singletonMap(FieldBoostMapAttribute.class.getName()+"#boosts", Collections.emptyMap()));
    _TestUtil.assertAttributeReflection(new FieldDateResolutionMapAttributeImpl(),
      Collections.singletonMap(FieldDateResolutionMapAttribute.class.getName()+"#dateRes", Collections.emptyMap()));
    _TestUtil.assertAttributeReflection(new FuzzyAttributeImpl(), new HashMap<String,Object>() {{
      put(FuzzyAttribute.class.getName()+"#prefixLength", FuzzyQuery.defaultPrefixLength);
      put(FuzzyAttribute.class.getName()+"#minSimilarity", FuzzyQuery.defaultMinSimilarity);
    }});
    _TestUtil.assertAttributeReflection(new LocaleAttributeImpl(),
      Collections.singletonMap(LocaleAttribute.class.getName()+"#locale", Locale.getDefault()));
    _TestUtil.assertAttributeReflection(new LowercaseExpandedTermsAttributeImpl(),
      Collections.singletonMap(LowercaseExpandedTermsAttribute.class.getName()+"#lowercaseExpandedTerms", true));
    _TestUtil.assertAttributeReflection(new MultiFieldAttributeImpl(),
      Collections.singletonMap(MultiFieldAttribute.class.getName()+"#fields", null));
    _TestUtil.assertAttributeReflection(new MultiTermRewriteMethodAttributeImpl(),
      Collections.singletonMap(MultiTermRewriteMethodAttribute.class.getName()+"#multiTermRewriteMethod", MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT));
    _TestUtil.assertAttributeReflection(new PositionIncrementsAttributeImpl(),
      Collections.singletonMap(PositionIncrementsAttribute.class.getName()+"#positionIncrementsEnabled", false));
    _TestUtil.assertAttributeReflection(new RangeCollatorAttributeImpl(),
      Collections.singletonMap(RangeCollatorAttribute.class.getName()+"#rangeCollator", null));
  }

}
