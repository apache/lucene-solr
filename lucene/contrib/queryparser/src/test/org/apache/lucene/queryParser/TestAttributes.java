package org.apache.lucene.queryParser;

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

import java.text.CollationKey;
import java.text.Collator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;

import junit.framework.Assert;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.queryParser.standard.config.AllowLeadingWildcardAttribute;
import org.apache.lucene.queryParser.standard.config.AnalyzerAttribute;
import org.apache.lucene.queryParser.standard.config.BoostAttribute;
import org.apache.lucene.queryParser.standard.config.DateResolutionAttribute;
import org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute;
import org.apache.lucene.queryParser.standard.config.DefaultPhraseSlopAttribute;
import org.apache.lucene.queryParser.standard.config.FieldBoostMapAttribute;
import org.apache.lucene.queryParser.standard.config.FieldDateResolutionMapAttribute;
import org.apache.lucene.queryParser.standard.config.FuzzyAttribute;
import org.apache.lucene.queryParser.standard.config.LocaleAttribute;
import org.apache.lucene.queryParser.standard.config.LowercaseExpandedTermsAttribute;
import org.apache.lucene.queryParser.standard.config.MultiFieldAttribute;
import org.apache.lucene.queryParser.standard.config.MultiTermRewriteMethodAttribute;
import org.apache.lucene.queryParser.standard.config.PositionIncrementsAttribute;
import org.apache.lucene.queryParser.standard.config.RangeCollatorAttribute;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestAttributes extends LuceneTestCase {

  @Test
  public void testAttributes() {
    StandardQueryConfigHandler config = new StandardQueryConfigHandler();

    AnalyzerAttribute analyzerAttr = config
        .addAttribute(AnalyzerAttribute.class);

    Assert.assertTrue(null == analyzerAttr.getAnalyzer());
    Assert.assertTrue(null == config.get(ConfigurationKeys.ANALYZER));
    Analyzer analyzer = new KeywordAnalyzer();
    analyzerAttr.setAnalyzer(analyzer);
    Assert.assertTrue(analyzer == analyzerAttr.getAnalyzer());
    Assert.assertTrue(analyzer == config.get(ConfigurationKeys.ANALYZER));

    DateResolutionAttribute dateResAttr = config
        .addAttribute(DateResolutionAttribute.class);

    Assert.assertTrue(null == dateResAttr.getDateResolution());
    Assert.assertTrue(null == config.get(ConfigurationKeys.DATE_RESOLUTION));
    DateTools.Resolution dateRes = DateTools.Resolution.HOUR;
    dateResAttr.setDateResolution(dateRes);
    Assert.assertTrue(dateRes == dateResAttr.getDateResolution());
    Assert.assertTrue(dateRes == config.get(ConfigurationKeys.DATE_RESOLUTION));

    DefaultPhraseSlopAttribute defaultPhraseSlopAttr = config
        .addAttribute(DefaultPhraseSlopAttribute.class);

    Assert.assertEquals(0, defaultPhraseSlopAttr.getDefaultPhraseSlop());
    Assert
        .assertEquals(0, config.get(ConfigurationKeys.PHRASE_SLOP).intValue());
    int phraseSlop = 1;
    defaultPhraseSlopAttr.setDefaultPhraseSlop(phraseSlop);
    Assert.assertEquals(phraseSlop, defaultPhraseSlopAttr
        .getDefaultPhraseSlop());
    Assert.assertEquals(phraseSlop, config.get(ConfigurationKeys.PHRASE_SLOP)
        .intValue());

    FieldBoostMapAttribute fieldBoostMapAttr = config
        .addAttribute(FieldBoostMapAttribute.class);

    Assert.assertEquals(new LinkedHashMap<String, Float>(), fieldBoostMapAttr
        .getFieldBoostMap());
    Assert.assertEquals(new LinkedHashMap<String, Float>(), config
        .get(ConfigurationKeys.FIELD_BOOST_MAP));
    LinkedHashMap<String, Float> fieldBoostMap = new LinkedHashMap<String, Float>();
    fieldBoostMap.put("test", 0.3f);
    fieldBoostMapAttr.setFieldBoostMap(fieldBoostMap);
    Assert.assertEquals(fieldBoostMap, fieldBoostMapAttr.getFieldBoostMap());
    Assert.assertEquals(fieldBoostMap, config
        .get(ConfigurationKeys.FIELD_BOOST_MAP));

    FieldDateResolutionMapAttribute fieldDateResolutionMapAttr = config
        .addAttribute(FieldDateResolutionMapAttribute.class);

    Assert.assertEquals(new HashMap<CharSequence, DateTools.Resolution>(),
        fieldDateResolutionMapAttr.getFieldDateResolutionMap());
    Assert.assertEquals(new HashMap<CharSequence, DateTools.Resolution>(),
        config.get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP));
    HashMap<CharSequence, DateTools.Resolution> fieldDateResMap = new HashMap<CharSequence, DateTools.Resolution>();
    fieldDateResMap.put("test", DateTools.Resolution.HOUR);
    fieldDateResolutionMapAttr.setFieldDateResolutionMap(fieldDateResMap);
    Assert.assertEquals(fieldDateResMap, fieldDateResolutionMapAttr.getFieldDateResolutionMap());
    Assert.assertEquals(fieldDateResMap, config
        .get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP));

    LocaleAttribute localeAttr = config.addAttribute(LocaleAttribute.class);

    Assert.assertEquals(Locale.getDefault(), localeAttr.getLocale());
    Assert.assertEquals(Locale.getDefault(), config
        .get(ConfigurationKeys.LOCALE));
    Locale locale = Locale.CHINA;
    localeAttr.setLocale(locale);
    Assert.assertEquals(locale, localeAttr.getLocale());
    Assert.assertEquals(locale, config.get(ConfigurationKeys.LOCALE));

    LowercaseExpandedTermsAttribute lowercaseExpandedTermsAttr = config
        .addAttribute(LowercaseExpandedTermsAttribute.class);

    Assert.assertEquals(true, lowercaseExpandedTermsAttr
        .isLowercaseExpandedTerms());
    Assert.assertEquals(true, config.get(
        ConfigurationKeys.LOWERCASE_EXPANDED_TERMS).booleanValue());
    boolean lowercaseEnabled = false;
    lowercaseExpandedTermsAttr.setLowercaseExpandedTerms(lowercaseEnabled);
    Assert.assertEquals(lowercaseEnabled, lowercaseExpandedTermsAttr
        .isLowercaseExpandedTerms());
    Assert.assertEquals(lowercaseEnabled, config.get(
        ConfigurationKeys.LOWERCASE_EXPANDED_TERMS).booleanValue());

    MultiTermRewriteMethodAttribute multiTermRewriteMethodAttr = config
        .addAttribute(MultiTermRewriteMethodAttribute.class);

    Assert
        .assertTrue(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT == multiTermRewriteMethodAttr
            .getMultiTermRewriteMethod());
    Assert
        .assertTrue(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT == config
            .get(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD));
    MultiTermQuery.RewriteMethod rewriteMethod = MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE;
    multiTermRewriteMethodAttr.setMultiTermRewriteMethod(rewriteMethod);
    Assert.assertTrue(rewriteMethod == multiTermRewriteMethodAttr
        .getMultiTermRewriteMethod());
    Assert.assertTrue(rewriteMethod == config
        .get(ConfigurationKeys.MULTI_TERM_REWRITE_METHOD));

    PositionIncrementsAttribute positionIncrementsAttr = config
        .addAttribute(PositionIncrementsAttribute.class);

    Assert.assertEquals(false, positionIncrementsAttr
        .isPositionIncrementsEnabled());
    Assert.assertEquals(false, config.get(
        ConfigurationKeys.ENABLE_POSITION_INCREMENTS).booleanValue());
    boolean posIncrEnabled = true;
    positionIncrementsAttr.setPositionIncrementsEnabled(posIncrEnabled);
    Assert.assertEquals(posIncrEnabled, positionIncrementsAttr
        .isPositionIncrementsEnabled());
    Assert.assertEquals(posIncrEnabled, config.get(
        ConfigurationKeys.ENABLE_POSITION_INCREMENTS).booleanValue());

    RangeCollatorAttribute rangeCollatorAttr = config
        .addAttribute(RangeCollatorAttribute.class);

    Assert.assertTrue(null == rangeCollatorAttr.getRangeCollator());
    Assert.assertTrue(null == config.get(ConfigurationKeys.RANGE_COLLATOR));
    Collator collator = new Collator() {

      @Override
      public int compare(String arg0, String arg1) {
        return 0;
      }

      @Override
      public CollationKey getCollationKey(String arg0) {
        return null;
      }

      @Override
      public int hashCode() {
        return 0;
      }

    };
    rangeCollatorAttr.setDateResolution(collator);
    Assert.assertTrue(collator == rangeCollatorAttr.getRangeCollator());
    Assert.assertTrue(collator == config.get(ConfigurationKeys.RANGE_COLLATOR));

    BoostAttribute boostAttr = config.addAttribute(BoostAttribute.class);

    Assert.assertEquals(1.0f, boostAttr.getBoost());
    Assert.assertEquals(1.0f, config.get(ConfigurationKeys.BOOST).floatValue());
    float boost = 2.0f;
    boostAttr.setBoost(boost);
    Assert.assertEquals(boost, boostAttr.getBoost());
    Assert
        .assertEquals(boost, config.get(ConfigurationKeys.BOOST).floatValue());

    FuzzyAttribute fuzzyAttributeAttr = config
        .addAttribute(FuzzyAttribute.class);

    Assert.assertEquals(FuzzyQuery.defaultMinSimilarity, fuzzyAttributeAttr
        .getFuzzyMinSimilarity());
    Assert.assertEquals(FuzzyQuery.defaultPrefixLength, fuzzyAttributeAttr
        .getPrefixLength());
    Assert.assertEquals(FuzzyQuery.defaultMinSimilarity, config.get(
        ConfigurationKeys.FUZZY_CONFIG).getMinSimilarity());
    Assert.assertEquals(FuzzyQuery.defaultPrefixLength, config.get(
        ConfigurationKeys.FUZZY_CONFIG).getPrefixLength());
    int prefixLength = 232;
    float minSim = 23.923f;
    fuzzyAttributeAttr.setFuzzyMinSimilarity(minSim);
    fuzzyAttributeAttr.setPrefixLength(prefixLength);
    Assert.assertEquals(minSim, fuzzyAttributeAttr.getFuzzyMinSimilarity());
    Assert.assertEquals(prefixLength, fuzzyAttributeAttr.getPrefixLength());
    Assert.assertEquals(minSim, config.get(ConfigurationKeys.FUZZY_CONFIG)
        .getMinSimilarity());
    Assert.assertEquals(prefixLength, config
        .get(ConfigurationKeys.FUZZY_CONFIG).getPrefixLength());

    DefaultOperatorAttribute defaultOpAttr = config
        .addAttribute(DefaultOperatorAttribute.class);

    Assert.assertEquals(DefaultOperatorAttribute.Operator.OR, defaultOpAttr
        .getOperator());
    Assert.assertEquals(Operator.OR, config
        .get(ConfigurationKeys.DEFAULT_OPERATOR));
    DefaultOperatorAttribute.Operator oldOperator = DefaultOperatorAttribute.Operator.AND;
    Operator newOperator = Operator.AND;
    defaultOpAttr.setOperator(oldOperator);
    Assert.assertEquals(oldOperator, defaultOpAttr.getOperator());
    Assert.assertEquals(newOperator, config
        .get(ConfigurationKeys.DEFAULT_OPERATOR));

  }

}
