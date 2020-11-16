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
package org.apache.lucene.misc.store;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Rule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.EnumSet;

public class WindowsDirectoryTest extends LuceneTestCase {
  @Rule
  public static TestRule requiresNative = new NativeLibEnableRule(
      EnumSet.of(NativeLibEnableRule.OperatingSystem.WINDOWS));

  public void testLibraryLoaded() throws IOException {
    try (Directory dir = new WindowsDirectory(RandomizedTest.newTempDir(LifecycleScope.TEST))) {
      dir.createOutput("test", IOContext.DEFAULT).close();
    }
  }
}