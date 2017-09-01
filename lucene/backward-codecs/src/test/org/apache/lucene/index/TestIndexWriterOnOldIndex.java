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
import java.nio.file.Path;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestIndexWriterOnOldIndex extends LuceneTestCase {

  public void testOpenModeAndCreatedVersion() throws IOException {
    assumeTrue("Reenable when 7.0 is released", false);
    InputStream resource = getClass().getResourceAsStream("unsupported.index.single-empty-doc.7.0.0.zip");
    assertNotNull(resource);
    Path path = createTempDir();
    TestUtil.unzip(resource, path);
    Directory dir = newFSDirectory(path);
    for (OpenMode openMode : OpenMode.values()) {
      Directory tmpDir = newDirectory(dir);
      assertEquals(7 /** 7.0.0 */, SegmentInfos.readLatestCommit(tmpDir).getIndexCreatedVersionMajor());
      IndexWriter w = new IndexWriter(tmpDir, newIndexWriterConfig().setOpenMode(openMode));
      w.commit();
      w.close();
      switch (openMode) {
        case CREATE:
          assertEquals(Version.LATEST.major, SegmentInfos.readLatestCommit(tmpDir).getIndexCreatedVersionMajor());
          break;
        default:
          assertEquals(7 /** 7.0.0 */, SegmentInfos.readLatestCommit(tmpDir).getIndexCreatedVersionMajor());
      }
      tmpDir.close();
    }
    dir.close();
  }

}
