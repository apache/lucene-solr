package org.apache.solr.core;

/**
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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Opens a directory with {@link LuceneTestCase#newFSDirectory(File)}
 */
public class MockFSDirectoryFactory extends CachingDirectoryFactory {

  @Override
  public Directory create(String path) throws IOException {
    MockDirectoryWrapper dir = LuceneTestCase.newFSDirectory(new File(path));
    // Somehow removing unref'd files in Solr tests causes
    // problems... there's some interaction w/
    // CachingDirectoryFactory.  Once we track down where Solr
    // isn't closing an IW, we can re-enable this:
    dir.setAssertNoUnrefencedFilesOnClose(false);
    return dir;
  }
}
