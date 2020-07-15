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

package org.apache.solr;

import java.lang.invoke.MethodHandles;
import java.io.File;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.StartupLoggingUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * All Solr test cases should derive from this class eventually. This is originally a result of async logging, see:
 * SOLR-12055 and associated. To enable async logging, we must gracefully shut down logging. Many Solr tests subclass
 * LuceneTestCase.
 *
 * Rather than add the cruft from SolrTestCaseJ4 to all the Solr tests that currently subclass LuceneTestCase,
 * we'll add the shutdown to this class and subclass it.
 *
 * Other changes that should affect every Solr test case may go here if they don't require the added capabilities in
 * SolrTestCaseJ4.
 */

  // ThreadLeakFilters are not additive. Any subclass that requires filters
  // other than these must redefine these as well.
@ThreadLeakFilters(defaultFilters = true, filters = {
        SolrIgnoredThreadsFilter.class,
        QuickPatchThreadsFilter.class
})
@ThreadLeakLingering(linger = 10000)
public class SolrTestCase extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ClassRule
  public static TestRule solrClassRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule())
             .around(new RevertDefaultThreadHandlerRule());

  /**
   * Sets the <code>solr.default.confdir</code> system property to the value of 
   * {@link ExternalPaths#DEFAULT_CONFIGSET} if and only if the system property is not already set, 
   * and the <code>DEFAULT_CONFIGSET</code> exists and is a readable directory.
   * <p>
   * Logs INFO/WARNing messages as appropriate based on these 2 conditions.
   * </p>
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  @BeforeClass
  public static void setDefaultConfigDirSysPropIfNotSet() {
    final String existingValue = System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    if (null != existingValue) {
      log.info("Test env includes configset dir system property '{}'='{}'", SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, existingValue);
      return;
    }
    final File extPath = new File(ExternalPaths.DEFAULT_CONFIGSET);
    if (extPath.canRead(/* implies exists() */) && extPath.isDirectory()) {
      log.info("Setting '{}' system property to test-framework derived value of '{}'",
               SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
      assert null == existingValue;
      System.setProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    } else {
      log.warn("System property '{}' is not already set, but test-framework derived value ('{}') either " +
               "does not exist or is not a readable directory, you may need to set the property yourself " +
               "for tests to run properly",
               SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    }
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link BeforeClass} method
   * @lucene.internal
   */
  @BeforeClass
  public static void checkSyspropForceBeforeClassAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.beforeclass=true"
    final String PROP = "tests.force.assumption.failure.beforeclass";
    assumeFalse(PROP + " == true",
                systemPropertyAsBoolean(PROP, false));
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link Before} method
   * @lucene.internal
   */
  @Before
  public void checkSyspropForceBeforeAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
    final String PROP = "tests.force.assumption.failure.before";
    assumeFalse(PROP + " == true",
                systemPropertyAsBoolean(PROP, false));
  }
  
  @AfterClass
  public static void shutdownLogger() throws Exception {
    StartupLoggingUtils.shutdown();
  }
}
