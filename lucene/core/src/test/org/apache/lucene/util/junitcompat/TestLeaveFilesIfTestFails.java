package org.apache.lucene.util.junitcompat;

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

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class TestLeaveFilesIfTestFails extends WithNestedTests {
  public TestLeaveFilesIfTestFails() {
    super(true);
  }
  
  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    static Path file;
    public void testDummy() {
      file = createTempDir("leftover");
      fail();
    }
  }

  @Test
  public void testLeaveFilesIfTestFails() throws IOException {
    Result r = JUnitCore.runClasses(Nested1.class);
    Assert.assertEquals(1, r.getFailureCount());
    Assert.assertTrue(Nested1.file != null && Files.exists(Nested1.file));
    Files.delete(Nested1.file);
  }
  
  public static class Nested2 extends WithNestedTests.AbstractNestedTest {
    static Path file;
    static Path parent;
    static SeekableByteChannel openFile;

    @SuppressWarnings("deprecation")
    public void testDummy() throws Exception {
      file = createTempDir("leftover").resolve("child.locked");
      openFile = Files.newByteChannel(file, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

      parent = LuceneTestCase.getBaseTempDirForTestClass();
    }
  }

  @Test
  public void testWindowsUnremovableFile() throws IOException {
    RandomizedTest.assumeTrue("Requires Windows.", Constants.WINDOWS);
    RandomizedTest.assumeFalse(LuceneTestCase.LEAVE_TEMPORARY);

    Result r = JUnitCore.runClasses(Nested2.class);
    Assert.assertEquals(1, r.getFailureCount());

    Nested2.openFile.close();
    IOUtils.rm(Nested2.parent);
  }  
}
