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
package org.apache.lucene.misc.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Command-line tool that enables listing segments in an
 * index, copying specific segments to another index, and
 * deleting segments from an index.
 *
 * <p>This tool does file-level copying of segments files.
 * This means it's unable to split apart a single segment
 * into multiple segments.  For example if your index is a
 * single segment, this tool won't help.  Also, it does basic
 * file-level copying (using simple
 * File{In,Out}putStream) so it will not work with non
 * FSDirectory Directory impls.</p>
 *
 * @lucene.experimental You can easily
 * accidentally remove segments from your index so be
 * careful!
 */
public class IndexSplitter {
  public final SegmentInfos infos;

  FSDirectory fsDir;

  Path dir;

  @SuppressForbidden(reason = "System.out required: command line tool")
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: IndexSplitter <srcDir> -l (list the segments and their sizes)");
      System.err.println("IndexSplitter <srcDir> <destDir> <segments>+");
      System.err
          .println("IndexSplitter <srcDir> -d (delete the following segments)");
      return;
    }
    Path srcDir = Paths.get(args[0]);
    IndexSplitter is = new IndexSplitter(srcDir);
    if (!Files.exists(srcDir)) {
      throw new Exception("srcdir:" + srcDir.toAbsolutePath()
          + " doesn't exist");
    }
    if (args[1].equals("-l")) {
      is.listSegments();
    } else if (args[1].equals("-d")) {
      List<String> segs = new ArrayList<>();
      for (int x = 2; x < args.length; x++) {
        segs.add(args[x]);
      }
      is.remove(segs.toArray(new String[0]));
    } else {
      Path targetDir = Paths.get(args[1]);
      List<String> segs = new ArrayList<>();
      for (int x = 2; x < args.length; x++) {
        segs.add(args[x]);
      }
      is.split(targetDir, segs.toArray(new String[0]));
    }
  }
  
  public IndexSplitter(Path dir) throws IOException {
    this.dir = dir;
    fsDir = FSDirectory.open(dir);
    infos = SegmentInfos.readLatestCommit(fsDir);
  }

  @SuppressForbidden(reason = "System.out required: command line tool")
  public void listSegments() throws IOException {
    DecimalFormat formatter = new DecimalFormat("###,###.###", DecimalFormatSymbols.getInstance(Locale.ROOT));
    for (int x = 0; x < infos.size(); x++) {
      SegmentCommitInfo info = infos.info(x);
      String sizeStr = formatter.format(info.sizeInBytes());
      System.out.println(info.info.name + " " + sizeStr);
    }
  }

  private SegmentCommitInfo getInfo(String name) {
    for (int x = 0; x < infos.size(); x++) {
      if (name.equals(infos.info(x).info.name))
        return infos.info(x);
    }
    return null;
  }

  public void remove(String[] segs) throws IOException {
    for (String n : segs) {
      SegmentCommitInfo info = getInfo(n);
      infos.remove(info);
    }
    infos.changed();
    infos.commit(fsDir);
  }

  public void split(Path destDir, String[] segs) throws IOException {
    Files.createDirectories(destDir);
    FSDirectory destFSDir = FSDirectory.open(destDir);
    SegmentInfos destInfos = new SegmentInfos(infos.getIndexCreatedVersionMajor());
    destInfos.counter = infos.counter;
    for (String n : segs) {
      SegmentCommitInfo infoPerCommit = getInfo(n);
      SegmentInfo info = infoPerCommit.info;
      // Same info just changing the dir:
      SegmentInfo newInfo = new SegmentInfo(destFSDir, info.getVersion(), info.getMinVersion(), info.name, info.maxDoc(),
                                            info.getUseCompoundFile(), info.getCodec(), info.getDiagnostics(), info.getId(), Collections.emptyMap(), null);
      destInfos.add(new SegmentCommitInfo(newInfo, infoPerCommit.getDelCount(), infoPerCommit.getSoftDelCount(),
          infoPerCommit.getDelGen(), infoPerCommit.getFieldInfosGen(),
          infoPerCommit.getDocValuesGen(), infoPerCommit.getId()));
      // now copy files over
      Collection<String> files = infoPerCommit.files();
      for (final String srcName : files) {
        Path srcFile = dir.resolve(srcName);
        Path destFile = destDir.resolve(srcName);
        Files.copy(srcFile, destFile);
      }
    }
    destInfos.changed();
    destInfos.commit(destFSDir);
    // System.out.println("destDir:"+destDir.getAbsolutePath());
  }
}
