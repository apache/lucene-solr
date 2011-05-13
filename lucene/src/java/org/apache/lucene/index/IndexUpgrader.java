package org.apache.lucene.index;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

/**
  * This is an easy-to-use tool that upgrades all segments of an index from previous Lucene versions
  * to the current segment file format. It can be used from command line:
  * <pre>
  *  java -cp lucene-core.jar org.apache.lucene.index.IndexUpgrader [-delete-prior-commits] [-verbose] indexDir
  * </pre>
  * Alternatively this class can be instantiated and {@link #upgrade} invoked. It uses {@link UpgradeIndexMergePolicy}
  * and triggers the upgrade via an optimize request to {@link IndexWriter}.
  * <p>This tool keeps only the last commit in an index; for this
  * reason, if the incoming index has more than one commit, the tool
  * refuses to run by default. Specify {@code -delete-prior-commits}
  * to override this, allowing the tool to delete all but the last commit.
  * From Java code this can be enabled by passing {@code true} to
  * {@link #IndexUpgrader(Directory,PrintStream,boolean)}.
  */
public final class IndexUpgrader {

  private static void printUsage() {
    System.err.println("Upgrades an index so all segments created with a previous Lucene version are rewritten.");
    System.err.println("Usage:");
    System.err.println("  java " + IndexUpgrader.class.getName() + " [-delete-prior-commits] [-verbose] indexDir");
    System.err.println("This tool keeps only the last commit in an index; for this");
    System.err.println("reason, if the incoming index has more than one commit, the tool");
    System.err.println("refuses to run by default. Specify -delete-prior-commits to override");
    System.err.println("this, allowing the tool to delete all but the last commit.");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException {
    String dir = null;
    boolean deletePriorCommits = false;
    PrintStream out = null;
    for (String arg : args) {
      if ("-delete-prior-commits".equals(arg)) {
        deletePriorCommits = true;
      } else if ("-verbose".equals(arg)) {
        out = System.out;
      } else if (dir == null) {
        dir = arg;
      } else {
        printUsage();
      }
    }
    if (dir == null) {
      printUsage();
    }
    
    new IndexUpgrader(FSDirectory.open(new File(dir)), out, deletePriorCommits).upgrade();
  }
  
  private final Directory dir;
  private final PrintStream infoStream;
  private final IndexWriterConfig iwc;
  private final boolean deletePriorCommits;
  
  @SuppressWarnings("deprecation")
  public IndexUpgrader(Directory dir) {
    this(dir, new IndexWriterConfig(Version.LUCENE_CURRENT, null), null, false);
  }
  
  @SuppressWarnings("deprecation")
  public IndexUpgrader(Directory dir, PrintStream infoStream, boolean deletePriorCommits) {
    this(dir, new IndexWriterConfig(Version.LUCENE_CURRENT, null), infoStream, deletePriorCommits);
  }
  
  public IndexUpgrader(Directory dir, IndexWriterConfig iwc, PrintStream infoStream, boolean deletePriorCommits) {
    this.dir = dir;
    this.iwc = iwc;
    this.infoStream = infoStream;
    this.deletePriorCommits = deletePriorCommits;
  }
  
  public void upgrade() throws IOException {
    if (!IndexReader.indexExists(dir)) {
      throw new IndexNotFoundException(dir.toString());
    }
  
    if (!deletePriorCommits) {
      final Collection<IndexCommit> commits = IndexReader.listCommits(dir);
      if (commits.size() > 1) {
        throw new IllegalArgumentException("This tool was invoked to not delete prior commit points, but the following commits were found: " + commits);
      }
    }
    
    final IndexWriterConfig c = (IndexWriterConfig) iwc.clone();
    c.setMergePolicy(new UpgradeIndexMergePolicy(c.getMergePolicy()));
    c.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    
    final IndexWriter w = new IndexWriter(dir, c);
    try {
      w.setInfoStream(infoStream);
      w.message("Upgrading all pre-" + Constants.LUCENE_MAIN_VERSION + " segments of index directory '" + dir + "' to version " + Constants.LUCENE_MAIN_VERSION + "...");
      w.optimize();
      w.message("All segments upgraded to version " + Constants.LUCENE_MAIN_VERSION);
    } finally {
      w.close();
    }
  }
  
}
