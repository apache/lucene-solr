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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.SSLTestConfig;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Path;
import java.util.Iterator;

public class SolrTestUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public SolrTestUtil() {
  }

  /**
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link Before} method
   *
   * @lucene.internal
   */
  //  @Before
  //  public void checkSyspropForceBeforeAssumptionFailure() {
  //    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
  //    final String PROP = "tests.force.assumption.failure.before";
  //    assumeFalse(PROP + " == true",
  //                systemPropertyAsBoolean(PROP, false));
  //  }
  public static String TEST_HOME() {
    return getFile("solr/collection1").getParent();
  }

  public static Path TEST_PATH() {
    return getFile("solr/collection1").getParentFile().toPath();
  }

  /**
   * Gets a resource from the context classloader as {@link File}. This method should only be used,
   * if a real file is needed. To get a stream, code should prefer
   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
   */
  public static File getFile(String name) {
    final URL url = SolrTestCaseJ4.class.getClassLoader().getResource(name.replace(File.separatorChar, '/'));
    if (url != null) {
      try {
        return new File(url.toURI());
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new RuntimeException("Resource was found on classpath, but cannot be resolved to a " + "normal file (maybe it is part of a JAR file): " + name);
      }
    }
    final File file = new File(name);
    if (file.exists()) {
      return file;
    }
    throw new RuntimeException("Cannot find resource in classpath or in file-system (relative to CWD): " + name);
  }

  /**
   * Return the name of the currently executing test case.
   */
  public static String getTestName() {
    return SolrTestCase.threadAndTestNameRule.testMethodName;
  }

  public static SSLTestConfig buildSSLConfig() {
    Class<?> targetClass = RandomizedContext.current().getTargetClass();
    final SolrTestCase.AlwaysUseSSL alwaysUseSSL = (SolrTestCase.AlwaysUseSSL) targetClass.getAnnotation(SolrTestCase.AlwaysUseSSL.class);
//    if (!LuceneTestCase.TEST_NIGHTLY && alwaysUseSSL == null) {
//      return new SSLTestConfig();
//    }
    // MRM TODO: whats up with SSL in nightly tests and http2 client?
    if (alwaysUseSSL == null) {
      return new SSLTestConfig();
    }

    RandomizeSSL.SSLRandomizer sslRandomizer = RandomizeSSL.SSLRandomizer.getSSLRandomizerForClass(targetClass);

    if (Constants.MAC_OS_X) {
      // see SOLR-9039
      // If a solution is found to remove this, please make sure to also update
      // TestMiniSolrCloudClusterSSL.testSslAndClientAuth as well.
      sslRandomizer = new RandomizeSSL.SSLRandomizer(sslRandomizer.ssl, 0.0D, (sslRandomizer.debug + " w/ MAC_OS_X supressed clientAuth"));
    }

    SSLTestConfig result = sslRandomizer.createSSLTestConfig();
    if (log.isInfoEnabled()) {
      log.info("Randomized ssl ({}) and clientAuth ({}) via: {}", result.isSSLMode(), result.isClientAuthMode(), sslRandomizer.debug);
    }
    return result;
  }

  /**
   * Creates an empty, temporary folder (when the name of the folder is of no importance).
   *
   * @see #createTempDir(String)
   */
  public static Path createTempDir() {
    return createTempDir("tempDir");
  }

  /**
   * Creates an empty, temporary folder with the given name prefix under the
   * test class's getBaseTempDirForTestClass().
   *
   * <p>The folder will be automatically removed after the
   * test class completes successfully. The test should close any file handles that would prevent
   * the folder from being removed.
   */
  public static Path createTempDir(String prefix) {
    return SolrTestCase.tempFilesCleanupRule.createTempDir(prefix);
  }

  /**
   * Creates an empty file with the given prefix and suffix under the
   * test class's getBaseTempDirForTestClass().
   *
   * <p>The file will be automatically removed after the
   * test class completes successfully. The test should close any file handles that would prevent
   * the folder from being removed.
   */
  public static Path createTempFile(String prefix, String suffix) throws IOException {
    return SolrTestCase.tempFilesCleanupRule.createTempFile(prefix, suffix);
  }

  /**
   * Creates an empty temporary file.
   *
   * @see #createTempFile(String, String)
   */
  public static Path createTempFile() throws IOException {
    return createTempFile("tempFile", ".tmp");
  }

  public static Path configset(String name) {
    return TEST_PATH().resolve("configsets").resolve(name).resolve("conf");
  }

  public static void wait(Thread thread) {
    if ((thread.getName().contains("ForkJoinPool.") || thread.getName().contains("Log4j2-")) && thread.getState() != Thread.State.TERMINATED) {
      log.info("Dont wait on ForkJoinPool. or Log4j2-");
      return;
    }

    do {
      log.warn("waiting on {} {}", thread.getName(), thread.getState());
      thread.interrupt();
      try {
        thread.join(10);
      } catch (InterruptedException e) {

      }
    } while (thread.isAlive());

  }

  public String getSaferTestName() {
    // test names can hold additional info, like the test seed
    // only take to first space
    String testName = SolrTestCase.threadAndTestNameRule.testMethodName;
    int index = testName.indexOf(' ');
    if (index > 0) {
      testName = testName.substring(0, index);
    }
    return testName;
  }

  public static boolean compareSolrDocumentList(Object expected, Object actual) {
    if (!(expected instanceof SolrDocumentList) || !(actual instanceof SolrDocumentList)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrDocumentList list1 = (SolrDocumentList) expected;
    SolrDocumentList list2 = (SolrDocumentList) actual;

    if (list1.getMaxScore() == null) {
      if (list2.getMaxScore() != null) {
        return false;
      }
    } else if (list2.getMaxScore() == null) {
      return false;
    } else {
      if (Float.compare(list1.getMaxScore(), list2.getMaxScore()) != 0 || list1.getNumFound() != list2.getNumFound() || list1.getStart() != list2.getStart()) {
        return false;
      }
    }
    for (int i = 0; i < list1.getNumFound(); i++) {
      if (!compareSolrDocument(list1.get(i), list2.get(i))) {
        return false;
      }
    }
    return true;
  }

  public static boolean compareSolrDocument(Object expected, Object actual) {

    if (!(expected instanceof SolrDocument) || !(actual instanceof SolrDocument)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrDocument solrDocument1 = (SolrDocument) expected;
    SolrDocument solrDocument2 = (SolrDocument) actual;

    if (solrDocument1.getFieldNames().size() != solrDocument2.getFieldNames().size()) {
      return false;
    }

    Iterator<String> iter1 = solrDocument1.getFieldNames().iterator();
    Iterator<String> iter2 = solrDocument2.getFieldNames().iterator();

    if (iter1.hasNext()) {
      String key1 = iter1.next();
      String key2 = iter2.next();

      Object val1 = solrDocument1.getFieldValues(key1);
      Object val2 = solrDocument2.getFieldValues(key2);

      if (!key1.equals(key2) || !val1.equals(val2)) {
        return false;
      }
    }

    if (solrDocument1.getChildDocuments() == null && solrDocument2.getChildDocuments() == null) {
      return true;
    }
    if (solrDocument1.getChildDocuments() == null || solrDocument2.getChildDocuments() == null) {
      return false;
    } else if (solrDocument1.getChildDocuments().size() != solrDocument2.getChildDocuments().size()) {
      return false;
    } else {
      Iterator<SolrDocument> childDocsIter1 = solrDocument1.getChildDocuments().iterator();
      Iterator<SolrDocument> childDocsIter2 = solrDocument2.getChildDocuments().iterator();
      while (childDocsIter1.hasNext()) {
        if (!compareSolrDocument(childDocsIter1.next(), childDocsIter2.next())) {
          return false;
        }
      }
      return true;
    }
  }

  public IndexableField newTextField(String value, String foo_bar_bar_bar_bar, Field.Store no) {
    return LuceneTestCase.newTextField(value, foo_bar_bar_bar_bar, no);
  }

  public static IndexSearcher newSearcher(IndexReader ir) {
    return LuceneTestCase.newSearcher(ir);
  }

  public static IndexableField newStringField(String value, String bar, Field.Store yes) {
    return LuceneTestCase.newStringField(value, bar, yes);
  }

  public static Directory newDirectory() {
    return LuceneTestCase.newDirectory();
  }

  public static int atLeast(int i) {
    return LuceneTestCase.atLeast(i);
  }


  public static abstract class StopableThread extends Thread {
    public abstract void stopThread();
  }

  public static abstract class HorridGC extends Thread {
    public abstract void stopHorridGC();
    public abstract void waitForThreads(int ms) throws InterruptedException;
  }

  public static HorridGC horridGC() {

    //          try {
    //            Thread.sleep(random().nextInt(10000));
    //          } catch (InterruptedException e) {
    //            throw new RuntimeException();
    //          }
    int delay = 10 + LuceneTestCase.random().nextInt(2000);
    StopableThread thread1 = new StopableThread() {
      volatile boolean stop = false;

      @Override
      public void run() {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException();
        }

        double sideEffect = 0;
        for (int i = 0; i < 60000; i++) {
          if (stop) {
            return;
          }
          sideEffect = slowpoke(599999L);
          if (stop) {
            return;
          }
          //          try {
          //            Thread.sleep(random().nextInt(10000));
          //          } catch (InterruptedException e) {
          //            throw new RuntimeException();
          //          }
        }
        System.out.println("result = " + sideEffect);
      }

      @Override
      public void stopThread() {
        this.stop = true;
      }
    };
    thread1.start();

    // trigger stop-the-world
    StopableThread thread2 = new StopableThread() {
      volatile boolean stop = false;

      @Override
      public void run() {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException();
        }
        int cnt = 0;
        long timestamp = System.currentTimeMillis();
        while (true) {
          if (cnt++ > 350) break;
          System.out.println("Delay " + (System.currentTimeMillis() - timestamp) + " " + cnt);

          timestamp = System.currentTimeMillis();
          // trigger stop-the-world
          System.gc();
          if (stop) {
            return;
          }
//          try {
//            Thread.sleep(LuceneTestCase.random().nextInt(3));
//          } catch (InterruptedException e) {
//            ParWork.propagateInterrupt(e);
//            throw new RuntimeException();
//          }
        }
      }

      @Override
      public void stopThread() {
        this.stop = true;
      }
    };
    thread2.start();
   return new HorridGC() {
     @Override
     public void stopHorridGC() {
       thread1.stopThread();
       thread2.stopThread();
     }

     @Override
     public void waitForThreads(int ms) throws InterruptedException {
       thread1.join(ms);
     }
   };
  }

  public static double slowpoke(long iterations) {
    double d = 0;
    for (int j = 1; j < iterations; j++) {
      d += Math.log(Math.E * j);
    }
    return d;
  }
}