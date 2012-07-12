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

package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import java.util.regex.Pattern;

import javax.script.ScriptEngineManager;

import org.junit.Assume;

public abstract class AbstractBadConfigTestBase extends SolrTestCaseJ4 {

  /**
   * Given a solrconfig.xml file name, a schema file name, and an 
   * expected errString, asserts that initializing a core with these 
   * files causes an error matching the specified errString ot be thrown.
   */
  protected final void assertConfigs(final String solrconfigFile,
                                     final String schemaFile, 
                                     final String errString) 
    throws Exception {

    ignoreException(Pattern.quote(errString));
    try {
      initCore( solrconfigFile, schemaFile );
    } catch (Exception e) {
      // short circuit out if we found what we expected
      if (-1 != e.getMessage().indexOf(errString)) return;
      // Test the cause too in case the expected error is wrapped by the TestHarness
      // (NOTE: we don't go all the way down. Either errString should be changed,
      // or some error wrapping should use a better message or both)
      if (null != e.getCause() &&
          null != e.getCause().getMessage() &&
          -1 != e.getCause().getMessage().indexOf(errString)) return;

      // otherwise, rethrow it, possibly completley unrelated
      throw new SolrException
        (ErrorCode.SERVER_ERROR, 
         "Unexpected error, expected error matching: " + errString, e);
    } finally {
      deleteCore();
      resetExceptionIgnores();
    }
    fail("Did not encounter any exception from: " + solrconfigFile + " using " + schemaFile);
  }

}
