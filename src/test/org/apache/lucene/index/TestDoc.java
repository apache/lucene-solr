package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;


import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.demo.FileDocument;

import java.io.*;
import java.util.*;


/** JUnit adaptation of an older test case DocTest.
 * @author dmitrys@earthlink.net
 * @version $Id$
 */
public class TestDoc extends TestCase {

    /** Main for running test case by itself. */
    public static void main(String args[]) {
        TestRunner.run (new TestSuite(TestDoc.class));
    }


    private File workDir;
    private File indexDir;
    private LinkedList files;


    /** Set the test case. This test case needs
     *  a few text files created in the current working directory.
     */
    public void setUp() throws IOException {
        workDir = new File(System.getProperty("tempDir"),"TestDoc");
        workDir.mkdirs();

        indexDir = new File(workDir, "testIndex");
        indexDir.mkdirs();

        Directory directory = FSDirectory.getDirectory(indexDir, true);
        directory.close();

        files = new LinkedList();
        files.add(createFile("test.txt",
            "This is the first test file"
        ));

        files.add(createFile("test2.txt",
            "This is the second test file"
        ));
    }

    private File createFile(String name, String text) throws IOException {
        FileWriter fw = null;
        PrintWriter pw = null;

        try {
            File f = new File(workDir, name);
            if (f.exists()) f.delete();

            fw = new FileWriter(f);
            pw = new PrintWriter(fw);
            pw.println(text);
            return f;

        } finally {
            if (pw != null) pw.close();
            if (fw != null) fw.close();
        }
    }


    /** This test executes a number of merges and compares the contents of
     *  the segments created when using compound file or not using one.
     *
     *  TODO: the original test used to print the segment contents to System.out
     *        for visual validation. To have the same effect, a new method
     *        checkSegment(String name, ...) should be created that would
     *        assert various things about the segment.
     */
    public void testIndexAndMerge() throws Exception {
      StringWriter sw = new StringWriter();
      PrintWriter out = new PrintWriter(sw, true);

      Directory directory = FSDirectory.getDirectory(indexDir, true);
      directory.close();

      indexDoc("one", "test.txt");
      printSegment(out, "one");

      indexDoc("two", "test2.txt");
      printSegment(out, "two");

      merge("one", "two", "merge", false);
      printSegment(out, "merge");

      merge("one", "two", "merge2", false);
      printSegment(out, "merge2");

      merge("merge", "merge2", "merge3", false);
      printSegment(out, "merge3");

      out.close();
      sw.close();
      String multiFileOutput = sw.getBuffer().toString();
      System.out.println(multiFileOutput);

      sw = new StringWriter();
      out = new PrintWriter(sw, true);

      directory = FSDirectory.getDirectory(indexDir, true);
      directory.close();

      indexDoc("one", "test.txt");
      printSegment(out, "one");

      indexDoc("two", "test2.txt");
      printSegment(out, "two");

      merge("one", "two", "merge", true);
      printSegment(out, "merge");

      merge("one", "two", "merge2", true);
      printSegment(out, "merge2");

      merge("merge", "merge2", "merge3", true);
      printSegment(out, "merge3");

      out.close();
      sw.close();
      String singleFileOutput = sw.getBuffer().toString();

      assertEquals(multiFileOutput, singleFileOutput);
   }


   private void indexDoc(String segment, String fileName)
   throws Exception
   {
      Directory directory = FSDirectory.getDirectory(indexDir, false);
      Analyzer analyzer = new SimpleAnalyzer();
      DocumentWriter writer =
         new DocumentWriter(directory, analyzer, Similarity.getDefault(), 1000);

      File file = new File(workDir, fileName);
      Document doc = FileDocument.Document(file);

      writer.addDocument(segment, doc);

      directory.close();
   }


   private void merge(String seg1, String seg2, String merged, boolean useCompoundFile)
   throws Exception {
      Directory directory = FSDirectory.getDirectory(indexDir, false);

      SegmentReader r1 = new SegmentReader(new SegmentInfo(seg1, 1, directory));
      SegmentReader r2 = new SegmentReader(new SegmentInfo(seg2, 1, directory));

      SegmentMerger merger =
        new SegmentMerger(directory, merged, useCompoundFile);

      merger.add(r1);
      merger.add(r2);
      merger.merge();

      directory.close();
   }


   private void printSegment(PrintWriter out, String segment)
   throws Exception {
      Directory directory = FSDirectory.getDirectory(indexDir, false);
      SegmentReader reader =
        new SegmentReader(new SegmentInfo(segment, 1, directory));

      for (int i = 0; i < reader.numDocs(); i++)
        out.println(reader.document(i));

      TermEnum tis = reader.terms();
      while (tis.next()) {
        out.print(tis.term());
        out.println(" DF=" + tis.docFreq());

        TermPositions positions = reader.termPositions(tis.term());
        try {
          while (positions.next()) {
            out.print(" doc=" + positions.doc());
            out.print(" TF=" + positions.freq());
            out.print(" pos=");
            out.print(positions.nextPosition());
            for (int j = 1; j < positions.freq(); j++)
              out.print("," + positions.nextPosition());
            out.println("");
          }
        } finally {
          positions.close();
        }
      }
      tis.close();
      reader.close();
      directory.close();
    }
}
