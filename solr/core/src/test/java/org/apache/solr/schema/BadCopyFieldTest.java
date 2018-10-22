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

/**
 * SOLR-4650: copyField source with no asterisk should trigger an error if it doesn't match an explicit or dynamic field 
 */
public class BadCopyFieldTest extends AbstractBadConfigTestBase {

  private void doTest(final String schema, final String errString) throws Exception {
    assertConfigs("solrconfig-basic.xml", schema, errString);
  }

  public void testNonGlobCopyFieldSourceMatchingNothingShouldFail() throws Exception {
    doTest("bad-schema-non-glob-copyfield-source-matching-nothing-should-fail-test.xml",
           "copyField source :'matches_nothing' is not a glob and doesn't match any explicit field or dynamicField."); 
  }

  private static final String INVALID_GLOB_MESSAGE = " is an invalid glob: either it contains more than one asterisk,"
                                                   + " or the asterisk occurs neither at the start nor at the end.";
  
  public void testMultipleAsteriskCopyFieldSourceShouldFail() throws Exception {
    doTest("bad-schema-multiple-asterisk-copyfield-source-should-fail-test.xml",
           "copyField source :'*too_many_asterisks*'" + INVALID_GLOB_MESSAGE);
  }

  public void testMisplacedAsteriskCopyFieldSourceShouldFail() throws Exception {
    doTest("bad-schema-misplaced-asterisk-copyfield-source-should-fail-test.xml",
           "copyField source :'misplaced_*_asterisk'" + INVALID_GLOB_MESSAGE);
  }

  public void testMultipleAsteriskCopyFieldDestShouldFail() throws Exception {
    doTest("bad-schema-multiple-asterisk-copyfield-dest-should-fail-test.xml",
           "copyField dest :'*too_many_asterisks*'" + INVALID_GLOB_MESSAGE);
  }

  public void testMisplacedAsteriskCopyFieldDestShouldFail() throws Exception {
    doTest("bad-schema-misplaced-asterisk-copyfield-dest-should-fail-test.xml",
           "copyField dest :'misplaced_*_asterisk'" + INVALID_GLOB_MESSAGE);
  }
}
