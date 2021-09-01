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
import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

// LUCENE-7501
public class TestManyPointsInOldIndex extends LuceneTestCase {

// To regenerate the back index zip:
//
// Compile:
//   1) temporarily remove 'extends LuceneTestCase' above (else java doesn't see our static void main)
//   2) ant compile-test
//
// Run:
//   1) java -cp ../build/backward-codecs/classes/test:../build/core/classes/java org.apache.lucene.index.TestManyPointsInOldIndex
//
//  cd manypointsindex
//  zip manypointsindex.zip *

  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get("manypointsindex"));
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    for(int i=0;i<1025;i++) {
      Document doc = new Document();
      doc.add(new IntPoint("intpoint", 1025-i));
      w.addDocument(doc);
    }
    w.close();
    dir.close();
  }

  public void testCheckOldIndex() throws IOException {
    assumeTrue("Reenable when 7.0 is released", false);
    Path path = createTempDir("manypointsindex");
    InputStream resource = getClass().getResourceAsStream("manypointsindex.zip");
    assertNotNull("manypointsindex not found", resource);
    TestUtil.unzip(resource, path);
    BaseDirectoryWrapper dir = newFSDirectory(path);
    // disable default checking...
    dir.setCheckIndexOnClose(false);

    // ... because we check ourselves here:
    TestUtil.checkIndex(dir, false, true, true, null);
    dir.close();
  }
}
