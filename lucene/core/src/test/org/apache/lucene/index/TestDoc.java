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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.LinkedList;
import java.util.Collection;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;


/** JUnit adaptation of an older test case DocTest. */
public class TestDoc extends LuceneTestCase {

    private File workDir;
    private File indexDir;
    private LinkedList<File> files;

    /** Set the test case. This test case needs
     *  a few text files created in the current working directory.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (VERBOSE) {
          System.out.println("TEST: setUp");
        }
        workDir = _TestUtil.getTempDir("TestDoc");
        workDir.mkdirs();

        indexDir = _TestUtil.getTempDir("testIndex");
        indexDir.mkdirs();

        Directory directory = newFSDirectory(indexDir);
        directory.close();

        files = new LinkedList<File>();
        files.add(createOutput("test.txt",
            "This is the first test file"
        ));

        files.add(createOutput("test2.txt",
            "This is the second test file"
        ));
    }

    private File createOutput(String name, String text) throws IOException {
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
      
      Directory directory = newFSDirectory(indexDir, null);
      IndexWriter writer = new IndexWriter(
          directory,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(-1).
              setMergePolicy(newLogMergePolicy(10))
      );

      SegmentInfo si1 = indexDoc(writer, "test.txt");
      printSegment(out, si1);

      SegmentInfo si2 = indexDoc(writer, "test2.txt");
      printSegment(out, si2);
      writer.close();

      SegmentInfo siMerge = merge(directory, si1, si2, "merge", false);
      printSegment(out, siMerge);

      SegmentInfo siMerge2 = merge(directory, si1, si2, "merge2", false);
      printSegment(out, siMerge2);

      SegmentInfo siMerge3 = merge(directory, siMerge, siMerge2, "merge3", false);
      printSegment(out, siMerge3);
      
      directory.close();
      out.close();
      sw.close();

      String multiFileOutput = sw.getBuffer().toString();
      //System.out.println(multiFileOutput);

      sw = new StringWriter();
      out = new PrintWriter(sw, true);

      directory = newFSDirectory(indexDir, null);
      writer = new IndexWriter(
          directory,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(-1).
              setMergePolicy(newLogMergePolicy(10))
      );

      si1 = indexDoc(writer, "test.txt");
      printSegment(out, si1);

      si2 = indexDoc(writer, "test2.txt");
      printSegment(out, si2);
      writer.close();

      siMerge = merge(directory, si1, si2, "merge", true);
      printSegment(out, siMerge);

      siMerge2 = merge(directory, si1, si2, "merge2", true);
      printSegment(out, siMerge2);

      siMerge3 = merge(directory, siMerge, siMerge2, "merge3", true);
      printSegment(out, siMerge3);
      
      directory.close();
      out.close();
      sw.close();
      String singleFileOutput = sw.getBuffer().toString();

      assertEquals(multiFileOutput, singleFileOutput);
   }

   private SegmentInfo indexDoc(IndexWriter writer, String fileName)
   throws Exception
   {
      File file = new File(workDir, fileName);
      Document doc = new Document();
      doc.add(new TextField("contents", new FileReader(file)));
      writer.addDocument(doc);
      writer.commit();
      return writer.newestSegment();
   }


   private SegmentInfo merge(Directory dir, SegmentInfo si1, SegmentInfo si2, String merged, boolean useCompoundFile)
   throws Exception {
      IOContext context = newIOContext(random());
      SegmentReader r1 = new SegmentReader(si1, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, context);
      SegmentReader r2 = new SegmentReader(si2, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, context);

      final Codec codec = Codec.getDefault();
      SegmentMerger merger = new SegmentMerger(InfoStream.getDefault(), si1.dir, IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL, merged, MergeState.CheckAbort.NONE, null, new FieldInfos(new FieldInfos.FieldNumberBiMap()), codec, context);

      merger.add(r1);
      merger.add(r2);
      MergeState mergeState = merger.merge();
      r1.close();
      r2.close();
      final FieldInfos fieldInfos =  mergeState.fieldInfos;
      final SegmentInfo info = new SegmentInfo(merged, si1.docCount + si2.docCount, si1.dir,
                                               false, codec, fieldInfos);
      
      if (useCompoundFile) {
        Collection<String> filesToDelete = IndexWriter.createCompoundFile(dir, merged + ".cfs", MergeState.CheckAbort.NONE, info, newIOContext(random()));
        info.setUseCompoundFile(true);
        for (final String fileToDelete : filesToDelete) 
          si1.dir.deleteFile(fileToDelete);
      }

      return info;
   }


   private void printSegment(PrintWriter out, SegmentInfo si)
   throws Exception {
      SegmentReader reader = new SegmentReader(si, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random()));

      for (int i = 0; i < reader.numDocs(); i++)
        out.println(reader.document(i));

      FieldsEnum fis = reader.fields().iterator();
      String field = fis.next();
      while(field != null)  {
        Terms terms = fis.terms();
        assertNotNull(terms);
        TermsEnum tis = terms.iterator(null);
        while(tis.next() != null) {

          out.print("  term=" + field + ":" + tis.term());
          out.println("    DF=" + tis.docFreq());

          DocsAndPositionsEnum positions = tis.docsAndPositions(reader.getLiveDocs(), null, false);

          while (positions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            out.print(" doc=" + positions.docID());
            out.print(" TF=" + positions.freq());
            out.print(" pos=");
            out.print(positions.nextPosition());
            for (int j = 1; j < positions.freq(); j++)
              out.print("," + positions.nextPosition());
            out.println("");
          }
        }
        field = fis.next();
      }
      reader.close();
    }
}
