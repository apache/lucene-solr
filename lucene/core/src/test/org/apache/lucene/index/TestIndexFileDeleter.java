package org.apache.lucene.index;

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

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestIndexFileDeleter extends LuceneTestCase {
  
  public void testDeleteLeftoverFiles() throws IOException {
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setPreventDoubleWrite(false);
    }

    LogMergePolicy mergePolicy = newLogMergePolicy(true, 10);
    
    // This test expects all of its segments to be in CFS
    mergePolicy.setNoCFSRatio(1.0);
    mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMaxBufferedDocs(10).
            setMergePolicy(mergePolicy)
    );

    int i;
    for(i=0;i<35;i++) {
      addDoc(writer, i);
    }
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setUseCompoundFile(false);
    for(;i<45;i++) {
      addDoc(writer, i);
    }
    writer.close();

    // Delete one doc so we get a .del file:
    writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES)
    );
    Term searchTerm = new Term("id", "7");
    writer.deleteDocuments(searchTerm);
    writer.close();

    // Now, artificially create an extra .del file & extra
    // .s0 file:
    String[] files = dir.listAll();

    /*
    for(int j=0;j<files.length;j++) {
      System.out.println(j + ": " + files[j]);
    }
    */

    // TODO: fix this test better
    String ext = Codec.getDefault().getName().equals("SimpleText") ? ".liv" : ".del";
    
    // Create a bogus separate del file for a
    // segment that already has a separate del file: 
    copyFile(dir, "_0_1" + ext, "_0_2" + ext);

    // Create a bogus separate del file for a
    // segment that does not yet have a separate del file:
    copyFile(dir, "_0_1" + ext, "_1_1" + ext);

    // Create a bogus separate del file for a
    // non-existent segment:
    copyFile(dir, "_0_1" + ext, "_188_1" + ext);

    // Create a bogus segment file:
    copyFile(dir, "_0.cfs", "_188.cfs");

    // Create a bogus fnm file when the CFS already exists:
    copyFile(dir, "_0.cfs", "_0.fnm");
    
    // Create some old segments file:
    copyFile(dir, "segments_2", "segments");
    copyFile(dir, "segments_2", "segments_1");

    // Create a bogus cfs file shadowing a non-cfs segment:
    
    // TODO: assert is bogus (relies upon codec-specific filenames)
    assertTrue(dir.fileExists("_3.fdt") || dir.fileExists("_3.fld"));
    assertTrue(!dir.fileExists("_3.cfs"));
    copyFile(dir, "_1.cfs", "_3.cfs");
    
    String[] filesPre = dir.listAll();

    // Open & close a writer: it should delete the above 4
    // files and nothing more:
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
    writer.close();

    String[] files2 = dir.listAll();
    dir.close();

    Arrays.sort(files);
    Arrays.sort(files2);
    
    Set<String> dif = difFiles(files, files2);
    
    if (!Arrays.equals(files, files2)) {
      fail("IndexFileDeleter failed to delete unreferenced extra files: should have deleted " + (filesPre.length-files.length) + " files but only deleted " + (filesPre.length - files2.length) + "; expected files:\n    " + asString(files) + "\n  actual files:\n    " + asString(files2)+"\ndiff: "+dif);
    }
  }

  private static Set<String> difFiles(String[] files1, String[] files2) {
    Set<String> set1 = new HashSet<String>();
    Set<String> set2 = new HashSet<String>();
    Set<String> extra = new HashSet<String>();
    
    for (int x=0; x < files1.length; x++) {
      set1.add(files1[x]);
    }
    for (int x=0; x < files2.length; x++) {
      set2.add(files2[x]);
    }
    Iterator<String> i1 = set1.iterator();
    while (i1.hasNext()) {
      String o = i1.next();
      if (!set2.contains(o)) {
        extra.add(o);
      }
    }
    Iterator<String> i2 = set2.iterator();
    while (i2.hasNext()) {
      String o = i2.next();
      if (!set1.contains(o)) {
        extra.add(o);
      }
    }
    return extra;
  }
  
  private String asString(String[] l) {
    String s = "";
    for(int i=0;i<l.length;i++) {
      if (i > 0) {
        s += "\n    ";
      }
      s += l[i];
    }
    return s;
  }

  public void copyFile(Directory dir, String src, String dest) throws IOException {
    IndexInput in = dir.openInput(src, newIOContext(random()));
    IndexOutput out = dir.createOutput(dest, newIOContext(random()));
    byte[] b = new byte[1024];
    long remainder = in.length();
    while(remainder > 0) {
      int len = (int) Math.min(b.length, remainder);
      in.readBytes(b, 0, len);
      out.writeBytes(b, len);
      remainder -= len;
    }
    in.close();
    out.close();
  }

  private void addDoc(IndexWriter writer, int id) throws IOException
  {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(newStringField("id", Integer.toString(id), Field.Store.NO));
    writer.addDocument(doc);
  }
}
