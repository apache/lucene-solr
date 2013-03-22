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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Constants;
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
        Writer fw = null;
        PrintWriter pw = null;

        try {
            File f = new File(workDir, name);
            if (f.exists()) f.delete();

            fw = new OutputStreamWriter(new FileOutputStream(f), "UTF-8");
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

      if (directory instanceof MockDirectoryWrapper) {
        // We create unreferenced files (we don't even write
        // a segments file):
        ((MockDirectoryWrapper) directory).setAssertNoUnrefencedFilesOnClose(false);
      }

      IndexWriter writer = new IndexWriter(
          directory,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(-1).
              setMergePolicy(newLogMergePolicy(10))
      );

      SegmentInfoPerCommit si1 = indexDoc(writer, "test.txt");
      printSegment(out, si1);

      SegmentInfoPerCommit si2 = indexDoc(writer, "test2.txt");
      printSegment(out, si2);
      writer.close();

      SegmentInfoPerCommit siMerge = merge(directory, si1, si2, "_merge", false);
      printSegment(out, siMerge);

      SegmentInfoPerCommit siMerge2 = merge(directory, si1, si2, "_merge2", false);
      printSegment(out, siMerge2);

      SegmentInfoPerCommit siMerge3 = merge(directory, siMerge, siMerge2, "_merge3", false);
      printSegment(out, siMerge3);
      
      directory.close();
      out.close();
      sw.close();

      String multiFileOutput = sw.getBuffer().toString();
      //System.out.println(multiFileOutput);

      sw = new StringWriter();
      out = new PrintWriter(sw, true);

      directory = newFSDirectory(indexDir, null);

      if (directory instanceof MockDirectoryWrapper) {
        // We create unreferenced files (we don't even write
        // a segments file):
        ((MockDirectoryWrapper) directory).setAssertNoUnrefencedFilesOnClose(false);
      }

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

      siMerge = merge(directory, si1, si2, "_merge", true);
      printSegment(out, siMerge);

      siMerge2 = merge(directory, si1, si2, "_merge2", true);
      printSegment(out, siMerge2);

      siMerge3 = merge(directory, siMerge, siMerge2, "_merge3", true);
      printSegment(out, siMerge3);
      
      directory.close();
      out.close();
      sw.close();
      String singleFileOutput = sw.getBuffer().toString();

      assertEquals(multiFileOutput, singleFileOutput);
   }

   private SegmentInfoPerCommit indexDoc(IndexWriter writer, String fileName)
   throws Exception
   {
      File file = new File(workDir, fileName);
      Document doc = new Document();
      InputStreamReader is = new InputStreamReader(new FileInputStream(file), "UTF-8");
      doc.add(new TextField("contents", is));
      writer.addDocument(doc);
      writer.commit();
      is.close();
      return writer.newestSegment();
   }


   private SegmentInfoPerCommit merge(Directory dir, SegmentInfoPerCommit si1, SegmentInfoPerCommit si2, String merged, boolean useCompoundFile)
   throws Exception {
      IOContext context = newIOContext(random());
      SegmentReader r1 = new SegmentReader(si1, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, context);
      SegmentReader r2 = new SegmentReader(si2, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, context);

      final Codec codec = Codec.getDefault();
      TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(si1.info.dir);
      final SegmentInfo si = new SegmentInfo(si1.info.dir, Constants.LUCENE_MAIN_VERSION, merged, -1, false, codec, null, null);

      SegmentMerger merger = new SegmentMerger(Arrays.<AtomicReader>asList(r1, r2),
          si, InfoStream.getDefault(), trackingDir, IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL,
          MergeState.CheckAbort.NONE, new FieldInfos.FieldNumbers(), context);

      MergeState mergeState = merger.merge();
      r1.close();
      r2.close();
      final SegmentInfo info = new SegmentInfo(si1.info.dir, Constants.LUCENE_MAIN_VERSION, merged,
                                               si1.info.getDocCount() + si2.info.getDocCount(),
                                               false, codec, null, null);
      info.setFiles(new HashSet<String>(trackingDir.getCreatedFiles()));
      
      if (useCompoundFile) {
        Collection<String> filesToDelete = IndexWriter.createCompoundFile(InfoStream.getDefault(), dir, MergeState.CheckAbort.NONE, info, newIOContext(random()));
        info.setUseCompoundFile(true);
        for (final String fileToDelete : filesToDelete) {
          si1.info.dir.deleteFile(fileToDelete);
        }
      }

      return new SegmentInfoPerCommit(info, 0, -1L);
   }


   private void printSegment(PrintWriter out, SegmentInfoPerCommit si)
   throws Exception {
      SegmentReader reader = new SegmentReader(si, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random()));

      for (int i = 0; i < reader.numDocs(); i++)
        out.println(reader.document(i));

      Fields fields = reader.fields();
      for (String field : fields)  {
        Terms terms = fields.terms(field);
        assertNotNull(terms);
        TermsEnum tis = terms.iterator(null);
        while(tis.next() != null) {

          out.print("  term=" + field + ":" + tis.term());
          out.println("    DF=" + tis.docFreq());

          DocsAndPositionsEnum positions = tis.docsAndPositions(reader.getLiveDocs(), null);

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
      }
      reader.close();
    }
}
