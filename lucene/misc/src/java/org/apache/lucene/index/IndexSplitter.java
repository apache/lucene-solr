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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.store.FSDirectory;

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
  public SegmentInfos infos;

  FSDirectory fsDir;

  File dir;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: IndexSplitter <srcDir> -l (list the segments and their sizes)");
      System.err.println("IndexSplitter <srcDir> <destDir> <segments>+");
      System.err
          .println("IndexSplitter <srcDir> -d (delete the following segments)");
      return;
    }
    File srcDir = new File(args[0]);
    IndexSplitter is = new IndexSplitter(srcDir);
    if (!srcDir.exists()) {
      throw new Exception("srcdir:" + srcDir.getAbsolutePath()
          + " doesn't exist");
    }
    if (args[1].equals("-l")) {
      is.listSegments();
    } else if (args[1].equals("-d")) {
      List<String> segs = new ArrayList<String>();
      for (int x = 2; x < args.length; x++) {
        segs.add(args[x]);
      }
      is.remove(segs.toArray(new String[0]));
    } else {
      File targetDir = new File(args[1]);
      List<String> segs = new ArrayList<String>();
      for (int x = 2; x < args.length; x++) {
        segs.add(args[x]);
      }
      is.split(targetDir, segs.toArray(new String[0]));
    }
  }
  
  public IndexSplitter(File dir) throws IOException {
    this.dir = dir;
    fsDir = FSDirectory.open(dir);
    infos = new SegmentInfos();
    infos.read(fsDir);
  }

  public void listSegments() throws IOException {
    DecimalFormat formatter = new DecimalFormat("###,###.###", DecimalFormatSymbols.getInstance(Locale.ROOT));
    for (int x = 0; x < infos.size(); x++) {
      SegmentInfoPerCommit info = infos.info(x);
      String sizeStr = formatter.format(info.sizeInBytes());
      System.out.println(info.info.name + " " + sizeStr);
    }
  }

  private int getIdx(String name) {
    for (int x = 0; x < infos.size(); x++) {
      if (name.equals(infos.info(x).info.name))
        return x;
    }
    return -1;
  }

  private SegmentInfoPerCommit getInfo(String name) {
    for (int x = 0; x < infos.size(); x++) {
      if (name.equals(infos.info(x).info.name))
        return infos.info(x);
    }
    return null;
  }

  public void remove(String[] segs) throws IOException {
    for (String n : segs) {
      int idx = getIdx(n);
      infos.remove(idx);
    }
    infos.changed();
    infos.commit(fsDir);
  }

  public void split(File destDir, String[] segs) throws IOException {
    destDir.mkdirs();
    FSDirectory destFSDir = FSDirectory.open(destDir);
    SegmentInfos destInfos = new SegmentInfos();
    destInfos.counter = infos.counter;
    for (String n : segs) {
      SegmentInfoPerCommit infoPerCommit = getInfo(n);
      SegmentInfo info = infoPerCommit.info;
      // Same info just changing the dir:
      SegmentInfo newInfo = new SegmentInfo(destFSDir, info.getVersion(), info.name, info.getDocCount(), 
                                            info.getUseCompoundFile(),
                                            info.getCodec(), info.getDiagnostics(), info.attributes());
      destInfos.add(new SegmentInfoPerCommit(newInfo, infoPerCommit.getDelCount(), infoPerCommit.getDelGen()));
      // now copy files over
      Collection<String> files = infoPerCommit.files();
      for (final String srcName : files) {
        File srcFile = new File(dir, srcName);
        File destFile = new File(destDir, srcName);
        copyFile(srcFile, destFile);
      }
    }
    destInfos.changed();
    destInfos.commit(destFSDir);
    // System.out.println("destDir:"+destDir.getAbsolutePath());
  }

  private static final byte[] copyBuffer = new byte[32*1024];

  private static void copyFile(File src, File dst) throws IOException {
    InputStream in = new FileInputStream(src);
    OutputStream out = new FileOutputStream(dst);
    int len;
    while ((len = in.read(copyBuffer)) > 0) {
      out.write(copyBuffer, 0, len);
    }
    in.close();
    out.close();
  }
}
