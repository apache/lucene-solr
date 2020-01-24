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
package org.apache.lucene.store;


import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;

import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.mockfile.WindowsFS;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;

/**
 * Tests SimpleFSDirectory
 */
public class TestSimpleFSDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new SimpleFSDirectory(path);
  }

  public void testRenameWithPendingDeletes() throws IOException {
    Path path = createTempDir();
    // irony: currently we don't emulate windows well enough to work on windows!
    assumeFalse("windows is not supported", Constants.WINDOWS);
    // Use WindowsFS to prevent open files from being deleted:
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path root = new FilterPath(path, fs);
    Directory directory = getDirectory(root);
    IndexOutput output = directory.createOutput("target.txt", IOContext.DEFAULT);
    output.writeInt(1);
    output.close();
    IndexOutput output1 = directory.createOutput("source.txt", IOContext.DEFAULT);
    output1.writeInt(2);
    output1.close();

    IndexInput input = directory.openInput("target.txt", IOContext.DEFAULT);
    directory.deleteFile("target.txt");
    directory.rename("source.txt", "target.txt");
    IndexInput input1 = directory.openInput("target.txt", IOContext.DEFAULT);
    assertTrue(directory.getPendingDeletions().isEmpty());
    assertEquals(1, input.readInt());
    assertEquals(2, input1.readInt());
    IOUtils.close(input1, input, directory);
  }

  public void testCreateOutputWithPendingDeletes() throws IOException {
    // irony: currently we don't emulate windows well enough to work on windows!
    assumeFalse("windows is not supported", Constants.WINDOWS);
    Path path = createTempDir();
    // Use WindowsFS to prevent open files from being deleted:
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path root = new FilterPath(path, fs);
    Directory directory = getDirectory(root);
    IndexOutput output = directory.createOutput("file.txt", IOContext.DEFAULT);
    output.writeInt(1);
    output.close();
    IndexInput input = directory.openInput("file.txt", IOContext.DEFAULT);
    directory.deleteFile("file.txt");
    expectThrows(IOException.class, () -> {
      directory.createOutput("file.txt", IOContext.DEFAULT);
    });
    assertTrue(directory.getPendingDeletions().isEmpty());
    assertEquals(1, input.readInt());
    IOUtils.close(input, directory);
  }
}
