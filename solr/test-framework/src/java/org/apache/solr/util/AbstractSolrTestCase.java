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


package org.apache.solr.util;


import java.io.File;
import java.util.HashSet;

import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * An Abstract base class that makes writing Solr JUnit tests "easier"
 *
 * <p>
 * Test classes that subclass this need only specify the path to the
 * schema.xml file (:TODO: the solrconfig.xml as well) and write some
 * testMethods.  This class takes care of creating/destroying the index,
 * and provides several assert methods to assist you.
 * </p>
 *
 * @see #setUp
 * @see #tearDown
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class
})
public abstract class AbstractSolrTestCase extends SolrTestCaseJ4 {
  protected SolrConfig solrConfig;


  /**
   * Subclasses can override this to change a test's solr home
   * (default is in test-files)
   */
  public String getSolrHome() {
    return SolrTestCaseJ4.TEST_HOME();
  }

  public static Logger log = LoggerFactory.getLogger(AbstractSolrTestCase.class);


    /** Causes an exception matching the regex pattern to not be logged. */
  public static void ignoreException(String pattern) {
    if (SolrException.ignorePatterns == null)
      SolrException.ignorePatterns = new HashSet<String>();
    SolrException.ignorePatterns.add(pattern);
  }

  public static void resetExceptionIgnores() {
    SolrException.ignorePatterns = null;
    ignoreException("ignore_exception");  // always ignore "ignore_exception"
  }

  /** Subclasses that override setUp can optionally call this method
   * to log the fact that their setUp process has ended.
   */
  public void postSetUp() {
    log.info("####POSTSETUP " + getTestName());
  }


  /** Subclasses that override tearDown can optionally call this method
   * to log the fact that the tearDown process has started.  This is necessary
   * since subclasses will want to call super.tearDown() at the *end* of their
   * tearDown method.
   */
  public void preTearDown() {
    log.info("####PRETEARDOWN " + getTestName());      
  }


  /**
   * Generates a simple &lt;add&gt;&lt;doc&gt;... XML String with the
   * commitWithin attribute.
   *
   * @param commitWithin the value of the commitWithin attribute 
   * @param fieldsAndValues 0th and Even numbered args are fields names odds are field values.
   * @see #add
   * @see #doc
   */
  public String adoc(int commitWithin, String... fieldsAndValues) {
    XmlDoc d = doc(fieldsAndValues);
    return add(d, "commitWithin", String.valueOf(commitWithin));
  }


  /**
   * Generates a &lt;delete&gt;... XML string for an ID
   *
   * @see TestHarness#deleteById
   */
  public String delI(String id, String... args) {
    return TestHarness.deleteById(id, args);
  }
  
  /**
   * Generates a &lt;delete&gt;... XML string for an query
   *
   * @see TestHarness#deleteByQuery
   */
  public String delQ(String q, String... args) {
    return TestHarness.deleteByQuery(q, args);
  }


  public static boolean recurseDelete(File f) {
    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!recurseDelete(sub)) {
          System.err.println("!!!! WARNING: best effort to remove " + sub.getAbsolutePath() + " FAILED !!!!!");
          return false;
        }
      }
    }
    return f.delete();
  }

  /** @see SolrTestCaseJ4#getFile */
  public static File getFile(String name) {
    return SolrTestCaseJ4.getFile(name);
  }
}
