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

package org.apache.solr.handler.component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.spelling.suggest.RandomTestDictionaryFactory;
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.apache.solr.update.SolrCoreState;
import org.junit.BeforeClass;
import org.junit.Test;

public class InfixSuggestersTest extends SolrTestCaseJ4 {
  private static final String rh_analyzing_short = "/suggest_analyzing_infix_short_dictionary";
  private static final String rh_analyzing_long = "/suggest_analyzing_infix_long_dictionary";
  private static final String rh_blended_short = "/suggest_blended_infix_short_dictionary";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-infixsuggesters.xml","schema.xml");
  }

  @Test
  public void test2xBuildReload() throws Exception {
    for (int i = 0 ; i < 2 ; ++i) {
      assertQ(req("qt", rh_analyzing_short,
          SuggesterParams.SUGGEST_BUILD_ALL, "true"),
          "//str[@name='command'][.='buildAll']"
      );
      h.reload();
    }
  }

  @Test
  public void testTwoSuggestersBuildThenReload() throws Exception {
    assertQ(req("qt", rh_analyzing_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();

    assertQ(req("qt", rh_blended_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
  }

  @Test
  public void testBuildThen2xReload() throws Exception {
    assertQ(req("qt", rh_analyzing_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
    h.reload();
  }

  @Test
  public void testAnalyzingInfixSuggesterBuildThenReload() throws Exception {
    assertQ(req("qt", rh_analyzing_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
  }

  @Test
  public void testBlendedInfixSuggesterBuildThenReload() throws Exception {
    assertQ(req("qt", rh_blended_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
  }

  @Test
  public void testReloadDuringBuild() throws Exception {
    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("InfixSuggesterTest");
    try {
      // Build the suggester in the background with a long dictionary
      @SuppressWarnings({"rawtypes"})
      Future job = executor.submit(() ->
          expectThrows(RuntimeException.class, SolrCoreState.CoreIsClosedException.class,
              () -> assertQ(req("qt", rh_analyzing_long,
                  SuggesterParams.SUGGEST_BUILD_ALL, "true"),
                  "//str[@name='command'][.='buildAll']")));
      h.reload();
      // Stop the dictionary's input iterator
      System.clearProperty(RandomTestDictionaryFactory.RandomTestDictionary
          .getEnabledSysProp("longRandomAnalyzingInfixSuggester"));
      job.get();
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  @Test
  public void testShutdownDuringBuild() throws Exception {
    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("InfixSuggesterTest");
    try {
      LinkedHashMap<Class<? extends Throwable>, List<Class<? extends Throwable>>> expected = new LinkedHashMap<>();
      expected.put(RuntimeException.class, Arrays.asList
          (SolrCoreState.CoreIsClosedException.class, SolrException.class, IllegalStateException.class, NullPointerException.class));
      final Throwable[] outerException = new Throwable[1];
      // Build the suggester in the background with a long dictionary
      @SuppressWarnings({"rawtypes"})
      Future job = executor.submit(() -> outerException[0] = expectThrowsAnyOf(expected,
          () -> assertQ(req("qt", rh_analyzing_long, SuggesterParams.SUGGEST_BUILD_ALL, "true"),
              "//str[@name='command'][.='buildAll']")));
      Thread.sleep(100); // TODO: is there a better way to ensure that the build has begun?
      h.close();
      // Stop the dictionary's input iterator
      System.clearProperty(RandomTestDictionaryFactory.RandomTestDictionary
          .getEnabledSysProp("longRandomAnalyzingInfixSuggester"));
      job.get();
      Throwable wrappedException = outerException[0].getCause();
      if (wrappedException instanceof SolrException) {
        String expectedMessage = "SolrCoreState already closed.";
        assertTrue("Expected wrapped SolrException message to contain '" + expectedMessage 
            + "' but message is '" + wrappedException.getMessage() + "'", 
            wrappedException.getMessage().contains(expectedMessage));
      } else if (wrappedException instanceof IllegalStateException
          && ! (wrappedException instanceof SolrCoreState.CoreIsClosedException)) { // CoreIsClosedException extends IllegalStateException
        String expectedMessage = "Cannot commit on an closed writer. Add documents first";
        assertTrue("Expected wrapped IllegalStateException message to contain '" + expectedMessage
                + "' but message is '" + wrappedException.getMessage() + "'",
            wrappedException.getMessage().contains(expectedMessage));
      }
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
      initCore("solrconfig-infixsuggesters.xml","schema.xml"); // put the core back for other tests
    }
  }
}
