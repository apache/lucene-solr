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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Iterator;

public class SolrTestUtil {
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
    if (!LuceneTestCase.TEST_NIGHTLY && alwaysUseSSL == null) {
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
    if (SolrTestCase.log.isInfoEnabled()) {
      SolrTestCase.log.info("Randomized ssl ({}) and clientAuth ({}) via: {}", result.isSSLMode(), result.isClientAuthMode(), sslRandomizer.debug);
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
    if (thread.getName().contains("ForkJoinPool.") || thread.getName().contains("Log4j2-")) {
      SolrTestCase.log.info("Dont wait on ForkJoinPool. or Log4j2-");
      return;
    }

    do {
      SolrTestCase.log.warn("waiting on {} {}", thread.getName(), thread.getState());
      try {
        thread.join(50);
      } catch (InterruptedException e) {

      }
      thread.interrupt();
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
}