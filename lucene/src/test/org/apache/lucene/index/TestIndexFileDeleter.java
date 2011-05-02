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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import java.io.*;
import java.util.*;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestIndexFileDeleter extends LuceneTestCase {
  
  public void testDeleteLeftoverFiles() throws IOException {
    MockDirectoryWrapper dir = newDirectory();
    dir.setPreventDoubleWrite(false);

    LogMergePolicy mergePolicy = newLogMergePolicy(true, 10);
    mergePolicy.setNoCFSRatio(1); // This test expects all of its segments to be in CFS

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMaxBufferedDocs(10).
            setMergePolicy(mergePolicy)
    );

    int i;
    for(i=0;i<35;i++) {
      addDoc(writer, i);
    }
    mergePolicy.setUseCompoundFile(false);
    for(;i<45;i++) {
      addDoc(writer, i);
    }
    writer.close();

    // Delete one doc so we get a .del file:
    IndexReader reader = IndexReader.open(dir, false);
    Term searchTerm = new Term("id", "7");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("didn't delete the right number of documents", 1, delCount);
    Similarity sim = new DefaultSimilarity();
    // Set one norm so we get a .s0 file:
    reader.setNorm(21, "content", sim.encodeNormValue(1.5f));
    reader.close();

    // Now, artificially create an extra .del file & extra
    // .s0 file:
    String[] files = dir.listAll();

    /*
    for(int j=0;j<files.length;j++) {
      System.out.println(j + ": " + files[j]);
    }
    */

    // The numbering of fields can vary depending on which
    // JRE is in use.  On some JREs we see content bound to
    // field 0; on others, field 1.  So, here we have to
    // figure out which field number corresponds to
    // "content", and then set our expected file names below
    // accordingly:
    CompoundFileReader cfsReader = new CompoundFileReader(dir, "_2.cfs");
    FieldInfos fieldInfos = new FieldInfos(cfsReader, "_2.fnm");
    int contentFieldIndex = -1;
    for (FieldInfo fi : fieldInfos) {
      if (fi.name.equals("content")) {
        contentFieldIndex = fi.number;
        break;
      }
    }
    cfsReader.close();
    assertTrue("could not locate the 'content' field number in the _2.cfs segment", contentFieldIndex != -1);

    String normSuffix = "s" + contentFieldIndex;

    // Create a bogus separate norms file for a
    // segment/field that actually has a separate norms file
    // already:
    copyFile(dir, "_2_1." + normSuffix, "_2_2." + normSuffix);

    // Create a bogus separate norms file for a
    // segment/field that actually has a separate norms file
    // already, using the "not compound file" extension:
    copyFile(dir, "_2_1." + normSuffix, "_2_2.f" + contentFieldIndex);

    // Create a bogus separate norms file for a
    // segment/field that does not have a separate norms
    // file already:
    copyFile(dir, "_2_1." + normSuffix, "_1_1." + normSuffix);

    // Create a bogus separate norms file for a
    // segment/field that does not have a separate norms
    // file already using the "not compound file" extension:
    copyFile(dir, "_2_1." + normSuffix, "_1_1.f" + contentFieldIndex);

    // Create a bogus separate del file for a
    // segment that already has a separate del file: 
    copyFile(dir, "_0_1.del", "_0_2.del");

    // Create a bogus separate del file for a
    // segment that does not yet have a separate del file:
    copyFile(dir, "_0_1.del", "_1_1.del");

    // Create a bogus separate del file for a
    // non-existent segment:
    copyFile(dir, "_0_1.del", "_188_1.del");

    // Create a bogus segment file:
    copyFile(dir, "_0.cfs", "_188.cfs");

    // Create a bogus fnm file when the CFS already exists:
    copyFile(dir, "_0.cfs", "_0.fnm");
    
    // Create some old segments file:
    copyFile(dir, "segments_2", "segments");
    copyFile(dir, "segments_2", "segments_1");

    // Create a bogus cfs file shadowing a non-cfs segment:
    copyFile(dir, "_1.cfs", "_2.cfs");
    
    String[] filesPre = dir.listAll();

    // Open & close a writer: it should delete the above 4
    // files and nothing more:
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    writer.close();

    String[] files2 = dir.listAll();
    dir.close();

    Arrays.sort(files);
    Arrays.sort(files2);
    
    Set<String> dif = difFiles(files, files2);
    
    if (!Arrays.equals(files, files2)) {
      fail("IndexFileDeleter failed to delete unreferenced extra files: should have deleted " + (filesPre.length-files.length) + " files but only deleted " + (filesPre.length - files2.length) + "; expected files:\n    " + asString(files) + "\n  actual files:\n    " + asString(files2)+"\ndif: "+dif);
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
    IndexInput in = dir.openInput(src);
    IndexOutput out = dir.createOutput(dest);
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
    doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(newField("id", Integer.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
  }
}
