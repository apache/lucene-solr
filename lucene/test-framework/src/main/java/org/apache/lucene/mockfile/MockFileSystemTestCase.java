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
package org.apache.lucene.mockfile;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;

/** 
 * Base class for testing mockfilesystems. This tests things
 * that really need to work: Path equals()/hashcode(), directory listing
 * glob and filtering, URI conversion, etc.
 */
@SuppressFileSystems("*") // we suppress random filesystems and do tests explicitly.
public abstract class MockFileSystemTestCase extends LuceneTestCase {
  
  /** wraps Path with custom behavior */
  protected abstract Path wrap(Path path);

  /** Test that Path.hashcode/equals are sane */
  public void testHashCodeEquals() throws IOException {
    Path dir = wrap(createTempDir());

    Path f1 = dir.resolve("file1");
    Path f1Again = dir.resolve("file1");
    Path f2 = dir.resolve("file2");
    
    assertEquals(f1, f1);
    assertFalse(f1.equals(null));
    assertEquals(f1, f1Again);
    assertEquals(f1.hashCode(), f1Again.hashCode());
    assertFalse(f1.equals(f2));
    dir.getFileSystem().close();
  }
  
  /** Test that URIs are not corrumpted */
  public void testURI() throws IOException {
    implTestURI("file1"); // plain ASCII
  }

  public void testURIumlaute() throws IOException {
    implTestURI("äÄöÖüÜß"); // Umlaute and s-zet
  }

  public void testURIchinese() throws IOException {
    implTestURI("中国"); // chinese
  }

  private void implTestURI(String fileName) throws IOException {
    assumeFalse("broken on J9: see https://issues.apache.org/jira/browse/LUCENE-6517", Constants.JAVA_VENDOR.startsWith("IBM"));
    Path dir = wrap(createTempDir());

    try {
      dir.resolve(fileName);
    } catch (InvalidPathException ipe) {
      assumeNoException("couldn't resolve '"+fileName+"'", ipe);
    }

    Path f1 = dir.resolve(fileName);
    URI uri = f1.toUri();
    Path f2 = dir.getFileSystem().provider().getPath(uri);
    assertEquals(f1, f2);

    dir.getFileSystem().close();
  }
  
  /** Tests that newDirectoryStream with a filter works correctly */
  public void testDirectoryStreamFiltered() throws IOException {
    Path dir = wrap(createTempDir());

    OutputStream file = Files.newOutputStream(dir.resolve("file1"));
    file.write(5);
    file.close();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      int count = 0;
      for (Path path : stream) {
        assertTrue(path instanceof FilterPath);
        if (!path.getFileName().toString().startsWith("extra")) {
          count++;
        }
      }
      assertEquals(1, count);
    }
    dir.getFileSystem().close();
  }

  /** Tests that newDirectoryStream with globbing works correctly */
  public void testDirectoryStreamGlobFiltered() throws IOException {
    Path dir = wrap(createTempDir());

    OutputStream file = Files.newOutputStream(dir.resolve("foo"));
    file.write(5);
    file.close();
    file = Files.newOutputStream(dir.resolve("bar"));
    file.write(5);
    file.close();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "f*")) {
      int count = 0;
      for (Path path : stream) {
        assertTrue(path instanceof FilterPath);
        ++count;
      }
      assertEquals(1, count);
    }
    dir.getFileSystem().close();
  }
}
