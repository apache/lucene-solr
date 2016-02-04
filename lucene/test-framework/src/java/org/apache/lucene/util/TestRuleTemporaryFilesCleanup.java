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
package org.apache.lucene.util;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.mockfile.DisableFsyncFS;
import org.apache.lucene.mockfile.ExtrasFS;
import org.apache.lucene.mockfile.HandleLimitFS;
import org.apache.lucene.mockfile.LeakFS;
import org.apache.lucene.mockfile.ShuffleFS;
import org.apache.lucene.mockfile.VerboseFS;
import org.apache.lucene.mockfile.WindowsFS;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.LuceneTestCase.SuppressFsync;
import org.apache.lucene.util.LuceneTestCase.SuppressTempFileChecks;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

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
  private Path javaTempDir;

  /**
   * Per-test class temporary folder.
   */
  private Path tempDirBase;
  
  /**
   * Per-test filesystem
   */
  private FileSystem fileSystem;

  /**
   * Suite failure marker.
   */
  private final TestRuleMarkFailure failureMarker;

  /**
   * A queue of temporary resources to be removed after the
   * suite completes.
   * @see #registerToRemoveAfterSuite(Path)
   */
  private final static List<Path> cleanupQueue = new ArrayList<Path>();

  public TestRuleTemporaryFilesCleanup(TestRuleMarkFailure failureMarker) {
    this.failureMarker = failureMarker;
  }

  /**
   * Register temporary folder for removal after the suite completes.
   */
  void registerToRemoveAfterSuite(Path f) {
    assert f != null;

    if (LuceneTestCase.LEAVE_TEMPORARY) {
      System.err.println("INFO: Will leave temporary file: " + f.toAbsolutePath());
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
    fileSystem = initializeFileSystem();
    javaTempDir = initializeJavaTempDir();

    // So all code using OfflineSorter (suggesters, BKD tree, NumericRangeTree) see MockFS goodness, e.g. catching leaked file handles:
    OfflineSorter.setDefaultTempDir(javaTempDir);
  }
  
  // os/config-independent limit for too many open files
  // TODO: can we make this lower?
  private static final int MAX_OPEN_FILES = 2048;
  
  private boolean allowed(Set<String> avoid, Class<? extends FileSystemProvider> clazz) {
    if (avoid.contains("*") || avoid.contains(clazz.getSimpleName())) {
      return false;
    } else {
      return true;
    }
  }
  
  private FileSystem initializeFileSystem() {
    Class<?> targetClass = RandomizedContext.current().getTargetClass();
    Set<String> avoid = new HashSet<>();
    if (targetClass.isAnnotationPresent(SuppressFileSystems.class)) {
      SuppressFileSystems a = targetClass.getAnnotation(SuppressFileSystems.class);
      avoid.addAll(Arrays.asList(a.value()));
    }
    FileSystem fs = FileSystems.getDefault();
    if (LuceneTestCase.VERBOSE && allowed(avoid, VerboseFS.class)) {
      fs = new VerboseFS(fs, new TestRuleSetupAndRestoreClassEnv.ThreadNameFixingPrintStreamInfoStream(System.out)).getFileSystem(null);
    }
    
    Random random = RandomizedContext.current().getRandom();
    
    // speed up tests by omitting actual fsync calls to the hardware most of the time.
    if (targetClass.isAnnotationPresent(SuppressFsync.class) || random.nextInt(100) > 0) {
      if (allowed(avoid, DisableFsyncFS.class)) {
        fs = new DisableFsyncFS(fs).getFileSystem(null);
      }
    }
    
    // impacts test reproducibility across platforms.
    if (random.nextInt(100) > 0) {
      if (allowed(avoid, ShuffleFS.class)) {
        fs = new ShuffleFS(fs, random.nextLong()).getFileSystem(null);
      }
    }
    
    // otherwise, wrap with mockfilesystems for additional checks. some 
    // of these have side effects (e.g. concurrency) so it doesn't always happen.
    if (random.nextInt(10) > 0) {
      if (allowed(avoid, LeakFS.class)) {
        fs = new LeakFS(fs).getFileSystem(null);
      }
      if (allowed(avoid, HandleLimitFS.class)) {
        fs = new HandleLimitFS(fs, MAX_OPEN_FILES).getFileSystem(null);
      }
      // windows is currently slow
      if (random.nextInt(10) == 0) {
        // don't try to emulate windows on windows: they don't get along
        if (!Constants.WINDOWS && allowed(avoid, WindowsFS.class)) {
          fs = new WindowsFS(fs).getFileSystem(null);
        }
      }
      if (allowed(avoid, ExtrasFS.class)) {
        fs = new ExtrasFS(fs, random.nextInt(4) == 0, random.nextBoolean()).getFileSystem(null);
      }
    }
    if (LuceneTestCase.VERBOSE) {
      System.out.println("filesystem: " + fs.provider());
    }
    return fs.provider().getFileSystem(URI.create("file:///"));
  }

  private Path initializeJavaTempDir() throws IOException {
    Path javaTempDir = fileSystem.getPath(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));
    
    Files.createDirectories(javaTempDir);

    assert Files.isDirectory(javaTempDir) &&
           Files.isWritable(javaTempDir);

    return javaTempDir.toRealPath();
  }

  @Override
  protected void afterAlways(List<Throwable> errors) throws Throwable {
    // Drain cleanup queue and clear it.
    final Path [] everything;
    final String tempDirBasePath;
    synchronized (cleanupQueue) {
      tempDirBasePath = (tempDirBase != null ? tempDirBase.toAbsolutePath().toString() : null);
      tempDirBase = null;

      Collections.reverse(cleanupQueue);
      everything = new Path [cleanupQueue.size()];
      cleanupQueue.toArray(everything);
      cleanupQueue.clear();
    }

    // Only check and throw an IOException on un-removable files if the test
    // was successful. Otherwise just report the path of temporary files
    // and leave them there.
    if (failureMarker.wasSuccessful()) {
      try {
        IOUtils.rm(everything);
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
      if (fileSystem != FileSystems.getDefault()) {
        fileSystem.close();
      }
    } else {
      if (tempDirBasePath != null) {
        System.err.println("NOTE: leaving temporary files on disk at: " + tempDirBasePath);
      }
    }
  }
  
  final Path getPerTestClassTempDir() {
    if (tempDirBase == null) {
      RandomizedContext ctx = RandomizedContext.current();
      Class<?> clazz = ctx.getTargetClass();
      String prefix = clazz.getName();
      prefix = prefix.replaceFirst("^org.apache.lucene.", "lucene.");
      prefix = prefix.replaceFirst("^org.apache.solr.", "solr.");

      int attempt = 0;
      Path f;
      boolean success = false;
      do {
        if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
          throw new RuntimeException(
              "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                + javaTempDir.toAbsolutePath());            
        }
        f = javaTempDir.resolve(prefix + "_" + ctx.getRunnerSeedAsString() 
              + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
        try {
          Files.createDirectory(f);
          success = true;
        } catch (IOException ignore) {}
      } while (!success);

      tempDirBase = f;
      registerToRemoveAfterSuite(tempDirBase);
    }
    return tempDirBase;
  }
  
  /**
   * @see LuceneTestCase#createTempDir()
   */
  public Path createTempDir(String prefix) {
    Path base = getPerTestClassTempDir();

    int attempt = 0;
    Path f;
    boolean success = false;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
              + base.toAbsolutePath());            
      }
      f = base.resolve(prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
      try {
        Files.createDirectory(f);
        success = true;
      } catch (IOException ignore) {}
    } while (!success);

    registerToRemoveAfterSuite(f);
    return f;
  }

  /**
   * @see LuceneTestCase#createTempFile()
   */
  public Path createTempFile(String prefix, String suffix) throws IOException {
    Path base = getPerTestClassTempDir();

    int attempt = 0;
    Path f;
    boolean success = false;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
              + base.toAbsolutePath());            
      }
      f = base.resolve(prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt) + suffix);
      try {
        Files.createFile(f);
        success = true;
      } catch (IOException ignore) {}
    } while (!success);

    registerToRemoveAfterSuite(f);
    return f;
  }
}
