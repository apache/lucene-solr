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

import java.util.Map;
import java.util.regex.Pattern;

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
    assertConfigs(solrconfigFile, schemaFile, null, errString);
  }

    /**
     * Given a solrconfig.xml file name, a schema file name, a solr home directory, 
     * and an expected errString, asserts that initializing a core with these 
     * files causes an error matching the specified errString ot be thrown.
     */
  protected final void assertConfigs(final String solrconfigFile,
                                     final String schemaFile,
                                     final String solrHome,
                                     final String errString) 
    throws Exception {

    ignoreException(Pattern.quote(errString));
    try {

      if (null == solrHome) {
        initCore( solrconfigFile, schemaFile );
      } else {
        initCore( solrconfigFile, schemaFile, solrHome );
      }

      CoreContainer cc = h.getCoreContainer();
      for (Map.Entry<String, CoreContainer.CoreLoadFailure> entry : cc.getCoreInitFailures().entrySet()) {
        if (matches(entry.getValue().exception, errString))
          return;
      }
    }
    catch (Exception e) {
      if (matches(e, errString))
        return;
      throw e;
    }
    finally {
      deleteCore();
      resetExceptionIgnores();
    }
    fail("Did not encounter any exception from: " + solrconfigFile + " using " + schemaFile);
  }

  private static boolean matches(Exception e, String errString) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t.getMessage() != null && -1 != t.getMessage().indexOf(errString))
        return true;
    }
    return false;
  }

}
