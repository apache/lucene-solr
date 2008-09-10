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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import java.io.*;
import java.util.*;
import java.util.zip.*;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestIndexFileDeleter extends LuceneTestCase
{
  public void testDeleteLeftoverFiles() throws IOException {

    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(10);
    int i;
    for(i=0;i<35;i++) {
      addDoc(writer, i);
    }
    writer.setUseCompoundFile(false);
    for(;i<45;i++) {
      addDoc(writer, i);
    }
    writer.close();

    // Delete one doc so we get a .del file:
    IndexReader reader = IndexReader.open(dir);
    Term searchTerm = new Term("id", "7");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("didn't delete the right number of documents", 1, delCount);

    // Set one norm so we get a .s0 file:
    reader.setNorm(21, "content", (float) 1.5);
    reader.close();

    // Now, artificially create an extra .del file & extra
    // .s0 file:
    String[] files = dir.list();

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
    for(i=0;i<fieldInfos.size();i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.name.equals("content")) {
        contentFieldIndex = i;
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

    // Create a deletable file:
    copyFile(dir, "_0.cfs", "deletable");

    // Create some old segments file:
    copyFile(dir, "segments_3", "segments");
    copyFile(dir, "segments_3", "segments_2");

    // Create a bogus cfs file shadowing a non-cfs segment:
    copyFile(dir, "_2.cfs", "_3.cfs");

    String[] filesPre = dir.list();

    // Open & close a writer: it should delete the above 4
    // files and nothing more:
    writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
    writer.close();

    String[] files2 = dir.list();
    dir.close();

    Arrays.sort(files);
    Arrays.sort(files2);

    if (!Arrays.equals(files, files2)) {
      fail("IndexFileDeleter failed to delete unreferenced extra files: should have deleted " + (filesPre.length-files.length) + " files but only deleted " + (filesPre.length - files2.length) + "; expected files:\n    " + asString(files) + "\n  actual files:\n    " + asString(files2));
    }
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
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("id", Integer.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
  }
}
