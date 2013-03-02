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

package org.apache.solr.schema;

import org.apache.solr.core.AbstractBadConfigTestBase;

public class BadIndexSchemaTest extends AbstractBadConfigTestBase {

  private void doTest(final String schema, final String errString) 
    throws Exception {
    assertConfigs("solrconfig.xml", schema, errString);
  }

  public void testSevereErrorsForInvalidFieldOptions() throws Exception {
    doTest("bad-schema-not-indexed-but-norms.xml", "bad_field");
    doTest("bad-schema-not-indexed-but-tf.xml", "bad_field");
    doTest("bad-schema-not-indexed-but-pos.xml", "bad_field");
    doTest("bad-schema-omit-tf-but-not-pos.xml", "bad_field");
  }

  public void testSevereErrorsForDuplicateFields() throws Exception {
    doTest("bad-schema-dup-field.xml", "fAgain");
  }

  public void testSevereErrorsForDuplicateDynamicField() throws Exception {
    doTest("bad-schema-dup-dynamicField.xml", "_twice");
  }

  public void testSevereErrorsForDuplicateFieldType() throws Exception {
    doTest("bad-schema-dup-fieldType.xml", "ftAgain");
  }

  public void testSevereErrorsForUnexpectedAnalyzer() throws Exception {
    doTest("bad-schema-nontext-analyzer.xml", "StrField (bad_type)");
    doTest("bad-schema-analyzer-class-and-nested.xml", "bad_type");
  }

  public void testBadExternalFileField() throws Exception {
    doTest("bad-schema-external-filefield.xml",
           "Only float and pfloat");
  }

  public void testUniqueKeyRules() throws Exception {
    doTest("bad-schema-uniquekey-is-copyfield-dest.xml", 
           "can not be the dest of a copyField");
    doTest("bad-schema-uniquekey-uses-default.xml", 
           "can not be configured with a default value");
    doTest("bad-schema-uniquekey-multivalued.xml", 
           "can not be configured to be multivalued");
  }

  public void testMultivaluedCurrency() throws Exception {
    doTest("bad-schema-currency-ft-multivalued.xml", 
           "types can not be multiValued: currency");
    doTest("bad-schema-currency-multivalued.xml", 
           "Fields can not be multiValued: money");
    doTest("bad-schema-currency-dynamic-multivalued.xml", 
           "Fields can not be multiValued: *_c");
  }

  public void testCurrencyOERNoRates() throws Exception {
    doTest("bad-schema-currency-ft-oer-norates.xml", 
           "ratesFileLocation");
  }

  public void testPerFieldtypeSimButNoSchemaSimFactory() throws Exception {
    doTest("bad-schema-sim-global-vs-ft-mismatch.xml", "global similarity does not support it");
  }
  
  public void testPerFieldtypePostingsFormatButNoSchemaCodecFactory() throws Exception {
    doTest("bad-schema-codec-global-vs-ft-mismatch.xml", "codec does not support");
  }

  public void testDocValuesNotRequiredNoDefault() throws Exception {
    doTest("bad-schema-docValues-not-required-no-default.xml", "has no default value and is not required");
  }

  public void testDocValuesUnsupported() throws Exception {
    doTest("bad-schema-unsupported-docValues.xml", "does not support doc values");
  }

  public void testSweetSpotSimBadConfig() throws Exception {
    doTest("bad-schema-sweetspot-both-tf.xml", "Can not mix");
    doTest("bad-schema-sweetspot-partial-baseline.xml", 
           "Overriding default baselineTf");
    doTest("bad-schema-sweetspot-partial-hyperbolic.xml", 
           "Overriding default hyperbolicTf");
    doTest("bad-schema-sweetspot-partial-norms.xml", 
           "Overriding default lengthNorm");
  }

}
