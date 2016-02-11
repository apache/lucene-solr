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
package org.apache.lucene.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.ProcessBuilder.Redirect;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.SeedUtils;

/**
 * Runs TestNRTThreads in a separate process, crashes the JRE in the middle
 * of execution, then runs checkindex to make sure it's not corrupt.
 */
public class TestIndexWriterOnJRECrash extends TestNRTThreads {
  private Path tempDir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("This test fails on UNIX with Turkish default locale (https://issues.apache.org/jira/browse/LUCENE-6036)",
      new Locale("tr").getLanguage().equals(Locale.getDefault().getLanguage()));
    tempDir = createTempDir("jrecrash");
  }
  
  @Override @Nightly
  public void testNRTThreads() throws Exception {
    // if we are not the fork
    if (System.getProperty("tests.crashmode") == null) {
      // try up to 10 times to create an index
      for (int i = 0; i < 10; i++) {
        forkTest();
        // if we succeeded in finding an index, we are done.
        if (checkIndexes(tempDir))
          return;
      }
    } else {
      // note: re-enable this if we create a 4.x impersonator,
      // and if its format is actually different than the real 4.x (unlikely)
      // TODO: the non-fork code could simply enable impersonation?
      // assumeFalse("does not support PreFlex, see LUCENE-3992", 
      //    Codec.getDefault().getName().equals("Lucene4x"));
      
      // we are the fork, setup a crashing thread
      final int crashTime = TestUtil.nextInt(random(), 3000, 4000);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(crashTime);
          } catch (InterruptedException e) {}
          crashJRE();
        }
      };
      t.setPriority(Thread.MAX_PRIORITY);
      t.start();
      // run the test until we crash.
      for (int i = 0; i < 1000; i++) {
        super.testNRTThreads();
      }
    }
  }
  
  /** fork ourselves in a new jvm. sets -Dtests.crashmode=true */
  @SuppressForbidden(reason = "ProcessBuilder requires java.io.File for CWD")
  public void forkTest() throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    cmd.add("-Xmx512m");
    cmd.add("-Dtests.crashmode=true");
    // passing NIGHTLY to this test makes it run for much longer, easier to catch it in the act...
    cmd.add("-Dtests.nightly=true");
    cmd.add("-DtempDir=" + tempDir);
    cmd.add("-Dtests.seed=" + SeedUtils.formatSeed(random().nextLong()));
    cmd.add("-ea");
    cmd.add("-cp");
    cmd.add(System.getProperty("java.class.path"));
    cmd.add("org.junit.runner.JUnitCore");
    cmd.add(getClass().getName());
    ProcessBuilder pb = new ProcessBuilder(cmd)
      .directory(tempDir.toFile())
      .redirectInput(Redirect.INHERIT)
      .redirectErrorStream(true);
    Process p = pb.start();

    // We pump everything to stderr.
    PrintStream childOut = System.err; 
    Thread stdoutPumper = ThreadPumper.start(p.getInputStream(), childOut);
    if (VERBOSE) childOut.println(">>> Begin subprocess output");
    p.waitFor();
    stdoutPumper.join();
    if (VERBOSE) childOut.println("<<< End subprocess output");
  }

  /** A pipe thread. It'd be nice to reuse guava's implementation for this... */
  static class ThreadPumper {
    public static Thread start(final InputStream from, final OutputStream to) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            byte[] buffer = new byte [1024];
            int len;
            while ((len = from.read(buffer)) != -1) {
              if (VERBOSE) {
                to.write(buffer, 0, len);
              }
            }
          } catch (IOException e) {
            System.err.println("Couldn't pipe from the forked process: " + e.toString());
          }
        }
      };
      t.start();
      return t;
    }
  }
  
  /**
   * Recursively looks for indexes underneath <code>file</code>,
   * and runs checkindex on them. returns true if it found any indexes.
   */
  public boolean checkIndexes(Path path) throws IOException {
    final AtomicBoolean found = new AtomicBoolean();
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult postVisitDirectory(Path dirPath, IOException exc) throws IOException {
        if (exc != null) {
          throw exc;
        } else {
          try (BaseDirectoryWrapper dir = newFSDirectory(dirPath)) {
            dir.setCheckIndexOnClose(false); // don't double-checkindex
            if (DirectoryReader.indexExists(dir)) {
              if (VERBOSE) {
                System.err.println("Checking index: " + dirPath);
              }
              // LUCENE-4738: if we crashed while writing first
              // commit it's possible index will be corrupt (by
              // design we don't try to be smart about this case
              // since that too risky):
              if (SegmentInfos.getLastCommitGeneration(dir) > 1) {
                TestUtil.checkIndex(dir);
              }
              found.set(true);
            }
          }
          return FileVisitResult.CONTINUE;
        }
      }
    });
    return found.get();
  }

  /**
   * currently, this only works/tested on Sun and IBM.
   */
  @SuppressForbidden(reason = "We need Unsafe to actually crush :-)")
  public void crashJRE() {
    final String vendor = Constants.JAVA_VENDOR;
    final boolean supportsUnsafeNpeDereference = 
        vendor.startsWith("Oracle") || 
        vendor.startsWith("Sun") || 
        vendor.startsWith("Apple");

    try {
      if (supportsUnsafeNpeDereference) {
        try {
          Class<?> clazz = Class.forName("sun.misc.Unsafe");
          Field field = clazz.getDeclaredField("theUnsafe");
          field.setAccessible(true);
          Object o = field.get(null);
          Method m = clazz.getMethod("putAddress", long.class, long.class);
          m.invoke(o, 0L, 0L);
        } catch (Throwable e) {
          System.out.println("Couldn't kill the JVM via Unsafe.");
          e.printStackTrace(System.out); 
        }
      }

      // Fallback attempt to Runtime.halt();
      Runtime.getRuntime().halt(-1);
    } catch (Exception e) {
      System.out.println("Couldn't kill the JVM.");
      e.printStackTrace(System.out); 
    }

    // We couldn't get the JVM to crash for some reason.
    fail();
  }
}
