package org.apache.lucene.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase.SuppressTempFileChecks;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

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

/**
 * Checks and cleans up temporary files.
 * 
 * @see LuceneTestCase#createTempDir()
 * @see LuceneTestCase#createTempFile()
 */
final class TestRuleTemporaryFilesCleanup extends TestRuleAdapter {
  /**
   * Retry to create temporary file name this many times.
   */
  private static final int TEMP_NAME_RETRY_THRESHOLD = 9999;

  /**
   * Writeable temporary base folder. 
   */
  private File javaTempDir;

  /**
   * Per-test class temporary folder.
   */
  private File tempDirBase;

  /**
   * Suite failure marker.
   */
  private final TestRuleMarkFailure failureMarker;

  /**
   * A queue of temporary resources to be removed after the
   * suite completes.
   * @see #registerToRemoveAfterSuite(File)
   */
  private final static List<File> cleanupQueue = new ArrayList<File>();

  public TestRuleTemporaryFilesCleanup(TestRuleMarkFailure failureMarker) {
    this.failureMarker = failureMarker;
  }

  /**
   * Register temporary folder for removal after the suite completes.
   */
  void registerToRemoveAfterSuite(File f) {
    assert f != null;

    if (LuceneTestCase.LEAVE_TEMPORARY) {
      System.err.println("INFO: Will leave temporary file: " + f.getAbsolutePath());
      return;
    }

    synchronized (cleanupQueue) {
      cleanupQueue.add(f);
    }
  }

  @Override
  protected void before() throws Throwable {
    super.before();

    assert tempDirBase == null;
    javaTempDir = initializeJavaTempDir();
  }

  private File initializeJavaTempDir() {
    File javaTempDir = new File(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));
    if (!javaTempDir.exists() && !javaTempDir.mkdirs()) {
      throw new RuntimeException("Could not create temp dir: " + javaTempDir.getAbsolutePath());
    }
    assert javaTempDir.isDirectory() &&
           javaTempDir.canWrite();

    return javaTempDir.getAbsoluteFile();
  }

  @Override
  protected void afterAlways(List<Throwable> errors) throws Throwable {
    // Drain cleanup queue and clear it.
    final File [] everything;
    final String tempDirBasePath;
    synchronized (cleanupQueue) {
      tempDirBasePath = (tempDirBase != null ? tempDirBase.getAbsolutePath() : null);
      tempDirBase = null;

      Collections.reverse(cleanupQueue);
      everything = new File [cleanupQueue.size()];
      cleanupQueue.toArray(everything);
      cleanupQueue.clear();
    }

    // Only check and throw an IOException on un-removable files if the test
    // was successful. Otherwise just report the path of temporary files
    // and leave them there.
    if (failureMarker.wasSuccessful()) {
      try {
        TestUtil.rm(everything);
      } catch (IOException e) {
        Class<?> suiteClass = RandomizedContext.current().getTargetClass();
        if (suiteClass.isAnnotationPresent(SuppressTempFileChecks.class)) {
          System.err.println("WARNING: Leftover undeleted temporary files (bugUrl: "
              + suiteClass.getAnnotation(SuppressTempFileChecks.class).bugUrl() + "): "
              + e.getMessage());
          return;
        }
        throw e;
      }
    } else {
      if (tempDirBasePath != null) {
        System.err.println("NOTE: leaving temporary files on disk at: " + tempDirBasePath);
      }
    }
  }
  
  final File getPerTestClassTempDir() {
    if (tempDirBase == null) {
      RandomizedContext ctx = RandomizedContext.current();
      Class<?> clazz = ctx.getTargetClass();
      String prefix = clazz.getName();
      prefix = prefix.replaceFirst("^org.apache.lucene.", "lucene.");
      prefix = prefix.replaceFirst("^org.apache.solr.", "solr.");

      int attempt = 0;
      File f;
      do {
        if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
          throw new RuntimeException(
              "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                + javaTempDir.getAbsolutePath());            
        }
        f = new File(javaTempDir, prefix + "-" + ctx.getRunnerSeedAsString() 
              + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
      } while (!f.mkdirs());

      tempDirBase = f;
      registerToRemoveAfterSuite(tempDirBase);
    }
    return tempDirBase;
  }

  /**
   * @see LuceneTestCase#createTempDir()
   */
  public File createTempDir(String prefix) {
    File base = getPerTestClassTempDir();

    int attempt = 0;
    File f;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
              + base.getAbsolutePath());            
      }
      f = new File(base, prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
    } while (!f.mkdirs());

    registerToRemoveAfterSuite(f);
    return f;
  }

  /**
   * @see LuceneTestCase#createTempFile()
   */
  public File createTempFile(String prefix, String suffix) throws IOException {
    File base = getPerTestClassTempDir();

    int attempt = 0;
    File f;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
              + base.getAbsolutePath());            
      }
      f = new File(base, prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt) + suffix);
    } while (!f.createNewFile());

    registerToRemoveAfterSuite(f);
    return f;
  }
}
