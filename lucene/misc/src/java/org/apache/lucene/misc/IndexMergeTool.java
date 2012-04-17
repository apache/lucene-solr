package org.apache.lucene.misc;

/**
  * Copyright 2005 The Apache Software Foundation
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;

/**
 * Merges indices specified on the command line into the index
 * specified as the first command line argument.
 */
public class IndexMergeTool {
  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: IndexMergeTool <mergedIndex> <index1> <index2> [index3] ...");
      System.exit(1);
    }
    FSDirectory mergedIndex = FSDirectory.open(new File(args[0]));

    IndexWriter writer = new IndexWriter(mergedIndex, new IndexWriterConfig(
        Version.LUCENE_CURRENT, null)
        .setOpenMode(OpenMode.CREATE));

    Directory[] indexes = new Directory[args.length - 1];
    for (int i = 1; i < args.length; i++) {
      indexes[i  - 1] = FSDirectory.open(new File(args[i]));
    }

    System.out.println("Merging...");
    writer.addIndexes(indexes);

    System.out.println("Full merge...");
    writer.forceMerge(1);
    writer.close();
    System.out.println("Done.");
  }
}
