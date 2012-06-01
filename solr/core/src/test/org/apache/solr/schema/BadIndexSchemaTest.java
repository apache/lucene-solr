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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrConfig;

import java.util.regex.Pattern;

import org.junit.Test;

public class BadIndexSchemaTest extends SolrTestCaseJ4 {

  private void doTest(final String schema, final String errString) 
    throws Exception {

    ignoreException(Pattern.quote(errString));
    try {
      initCore( "solrconfig.xml", schema );
    } catch (SolrException e) {
      // short circuit out if we found what we expected
      if (-1 != e.getMessage().indexOf(errString)) return;
      // Test the cause too in case the expected error is wrapped
      if (null != e.getCause() && 
          -1 != e.getCause().getMessage().indexOf(errString)) return;

      // otherwise, rethrow it, possibly completley unrelated
      throw new SolrException
        (ErrorCode.SERVER_ERROR, 
         "Unexpected error, expected error matching: " + errString, e);
    } finally {
      deleteCore();
    }
    fail("Did not encounter any exception from: " + schema);
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
  }

}
