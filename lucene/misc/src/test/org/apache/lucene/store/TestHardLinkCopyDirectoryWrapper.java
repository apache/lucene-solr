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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.util.IOUtils;

public class TestHardLinkCopyDirectoryWrapper extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path file) throws IOException {
    Directory open;
    if (random().nextBoolean()) {
      open = new RAMDirectory();
    } else {
      open = FSDirectory.open(file);
    }
    return new HardlinkCopyDirectoryWrapper(open);
  }

  /**
   * Tests that we use hardlinks if possible on Directory#copyFrom
   */
  public void testCopyHardLinks() throws IOException {
    Path tempDir = createTempDir();
    Path dir_1 = tempDir.resolve("dir_1");
    Path dir_2 = tempDir.resolve("dir_2");
    Files.createDirectories(dir_1);
    Files.createDirectories(dir_2);

    Directory luceneDir_1 = newFSDirectory(dir_1);
    Directory luceneDir_2 = newFSDirectory(dir_2);
    try {
      try (IndexOutput output = luceneDir_1.createOutput("foo.bar", IOContext.DEFAULT)) {
        CodecUtil.writeHeader(output, "foo", 0);
        output.writeString("hey man, nice shot!");
        CodecUtil.writeFooter(output);
      }
      // In case luceneDir_1 has an NRTCachingDirectory
      luceneDir_1.sync(Collections.singleton("foo.bar"));
      try {
        Files.createLink(tempDir.resolve("test"), dir_1.resolve("foo.bar"));
        BasicFileAttributes destAttr = Files.readAttributes(tempDir.resolve("test"), BasicFileAttributes.class);
        BasicFileAttributes sourceAttr = Files.readAttributes(dir_1.resolve("foo.bar"), BasicFileAttributes.class);
        assumeTrue("hardlinks are not supported", destAttr.fileKey() != null
            && destAttr.fileKey().equals(sourceAttr.fileKey()));
      } catch (UnsupportedOperationException ex) {
        assumeFalse("hardlinks are not supported", true);
      }

      HardlinkCopyDirectoryWrapper wrapper = new HardlinkCopyDirectoryWrapper(luceneDir_2);
      wrapper.copyFrom(luceneDir_1, "foo.bar", "bar.foo", IOContext.DEFAULT);
      assertTrue(Files.exists(dir_2.resolve("bar.foo")));
      BasicFileAttributes destAttr = Files.readAttributes(dir_2.resolve("bar.foo"), BasicFileAttributes.class);
      BasicFileAttributes sourceAttr = Files.readAttributes(dir_1.resolve("foo.bar"), BasicFileAttributes.class);
      assertEquals(destAttr.fileKey(), sourceAttr.fileKey());
      try (ChecksumIndexInput indexInput = wrapper.openChecksumInput("bar.foo", IOContext.DEFAULT)) {
        CodecUtil.checkHeader(indexInput, "foo", 0, 0);
        assertEquals("hey man, nice shot!", indexInput.readString());
        CodecUtil.checkFooter(indexInput);
      }
    } finally {
      // close them in a finally block we might run into an assume here
      IOUtils.close(luceneDir_1, luceneDir_2);
    }

  }
}
