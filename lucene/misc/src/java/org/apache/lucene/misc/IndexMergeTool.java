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
package org.apache.lucene.misc;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.util.SuppressForbidden;

import java.nio.file.Paths;

/**
 * Merges indices specified on the command line into the index
 * specified as the first command line argument.
 */
@SuppressForbidden(reason = "System.out required: command line tool")
public class IndexMergeTool {
  
  static final String USAGE =
    "Usage: IndexMergeTool [OPTION...] <mergedIndex> <index1> <index2> [index3] ...\n" +
    "Merges source indexes 'index1' .. 'indexN' into 'mergedIndex'\n" +
    "\n" +
    "OPTIONS:\n" +
    " -merge-policy ClassName  specifies MergePolicy class (must be in CLASSPATH).The default is\n" +
    "                          'org.apache.lucene.index.TieredMergePolicy.TieredMergePolicy'\n" +
    " -max-segments N          force-merge's the index to a maximum of N segments. Default is\n" +
    "                          to execute only the merges according to the merge policy.\n" +
    " -verbose                 print additional details.\n";

  @SuppressForbidden(reason = "System.err required (verbose mode): command line tool")
  static class Options {
    String mergedIndexPath;
    String indexPaths[];
    IndexWriterConfig config = new IndexWriterConfig(null).setOpenMode(OpenMode.CREATE);
    int maxSegments = 0;

    static Options parse(String args[]) throws ReflectiveOperationException {
      Options options = new Options();
      int index = 0;
      while (index < args.length) {
        if (!args[index].startsWith("-")) {
          break;
        }
        if (args[index] == "--") {
          break;
        }
        switch(args[index]) {
          case "-merge-policy":
            String clazzName = args[++index];
            Class<? extends MergePolicy> clazz = Class.forName(clazzName).asSubclass(MergePolicy.class);
            options.config.setMergePolicy(clazz.getConstructor().newInstance());
            break;
          case "-max-segments":
            options.maxSegments = Integer.parseInt(args[++index]);
            break;
          case "-verbose":
            options.config.setInfoStream(System.err);
            break;
          default: throw new IllegalArgumentException("unrecognized option: '" + args[index] + "'\n" + USAGE);
        }
        index++;
      }

      // process any remaining arguments as the target and source index paths.
      int numPaths = args.length - index;
      if (numPaths < 3) {
        throw new IllegalArgumentException("not enough parameters.\n" + USAGE);
      }

      options.mergedIndexPath = args[index];
      options.indexPaths = new String[numPaths - 1];
      System.arraycopy(args, index + 1, options.indexPaths, 0, options.indexPaths.length);
      return options;
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = null;
    try {
      options = Options.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }

    // Try to use hardlinks to source segments, if possible.
    Directory mergedIndex = new HardlinkCopyDirectoryWrapper(FSDirectory.open(Paths.get(options.mergedIndexPath)));

    Directory[] indexes = new Directory[options.indexPaths.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = FSDirectory.open(Paths.get(options.indexPaths[i]));
    }

    IndexWriter writer = new IndexWriter(mergedIndex, options.config);

    System.out.println("Merging...");
    writer.addIndexes(indexes);

    if (options.maxSegments > 0) {
      System.out.println("Force-merging to " + options.maxSegments + "...");
      writer.forceMerge(options.maxSegments);
    }
    writer.close();
    System.out.println("Done.");
  }
  
}
