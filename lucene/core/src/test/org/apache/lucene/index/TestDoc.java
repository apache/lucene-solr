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


import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/** JUnit adaptation of an older test case DocTest. */
public class TestDoc extends LuceneTestCase {

  private Path workDir;
  private Path indexDir;
  private LinkedList<Path> files;

  /** Set the test case. This test case needs
   *  a few text files created in the current working directory.
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    workDir = createTempDir("TestDoc");
    indexDir = createTempDir("testIndex");

    Directory directory = newFSDirectory(indexDir);
    directory.close();

    files = new LinkedList<>();
    files.add(createOutput("test.txt",
                           "This is the first test file"
                           ));

    files.add(createOutput("test2.txt",
                           "This is the second test file"
                           ));
  }

  private Path createOutput(String name, String text) throws IOException {
    Writer fw = null;
    PrintWriter pw = null;

    try {
      Path path = workDir.resolve(name);
      Files.deleteIfExists(path);

      fw = new OutputStreamWriter(Files.newOutputStream(path), StandardCharsets.UTF_8);
      pw = new PrintWriter(fw);
      pw.println(text);
      return path;

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
      
    Directory directory = newFSDirectory(indexDir);

    if (directory instanceof MockDirectoryWrapper) {
      // We create unreferenced files (we don't even write
      // a segments file):
      ((MockDirectoryWrapper) directory).setAssertNoUnrefencedFilesOnClose(false);
    }

    IndexWriter writer = new IndexWriter(
                                         directory,
                                         newIndexWriterConfig(new MockAnalyzer(random())).
                                         setOpenMode(OpenMode.CREATE).
                                         setMaxBufferedDocs(-1).
                                         setMergePolicy(newLogMergePolicy(10))
                                         );

    SegmentCommitInfo si1 = indexDoc(writer, "test.txt");
    printSegment(out, si1);

    SegmentCommitInfo si2 = indexDoc(writer, "test2.txt");
    printSegment(out, si2);
    writer.close();

    SegmentCommitInfo siMerge = merge(directory, si1, si2, "_merge", false);
    printSegment(out, siMerge);

    SegmentCommitInfo siMerge2 = merge(directory, si1, si2, "_merge2", false);
    printSegment(out, siMerge2);

    SegmentCommitInfo siMerge3 = merge(directory, siMerge, siMerge2, "_merge3", false);
    printSegment(out, siMerge3);
      
    directory.close();
    out.close();
    sw.close();

    String multiFileOutput = sw.toString();
    //System.out.println(multiFileOutput);

    sw = new StringWriter();
    out = new PrintWriter(sw, true);

    directory = newFSDirectory(indexDir);

    if (directory instanceof MockDirectoryWrapper) {
      // We create unreferenced files (we don't even write
      // a segments file):
      ((MockDirectoryWrapper) directory).setAssertNoUnrefencedFilesOnClose(false);
    }

    writer = new IndexWriter(
                             directory,
                             newIndexWriterConfig(new MockAnalyzer(random())).
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
    String singleFileOutput = sw.toString();

    assertEquals(multiFileOutput, singleFileOutput);
  }

  private SegmentCommitInfo indexDoc(IndexWriter writer, String fileName)
    throws Exception
  {
    Path path = workDir.resolve(fileName);
    Document doc = new Document();
    InputStreamReader is = new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8);
    doc.add(new TextField("contents", is));
    writer.addDocument(doc);
    writer.commit();
    is.close();
    return writer.newestSegment();
  }


  private SegmentCommitInfo merge(Directory dir, SegmentCommitInfo si1, SegmentCommitInfo si2, String merged, boolean useCompoundFile)
    throws Exception {
    IOContext context = newIOContext(random(), new IOContext(new MergeInfo(-1, -1, false, -1)));
    SegmentReader r1 = new SegmentReader(si1, Version.LATEST.major, context);
    SegmentReader r2 = new SegmentReader(si2, Version.LATEST.major, context);

    final Codec codec = Codec.getDefault();
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(si1.info.dir);
    final SegmentInfo si = new SegmentInfo(si1.info.dir, Version.LATEST, null, merged, -1, false, codec, Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);

    SegmentMerger merger = new SegmentMerger(Arrays.<CodecReader>asList(r1, r2),
                                             si, InfoStream.getDefault(), trackingDir,
                                             new FieldInfos.FieldNumbers(), context);

    MergeState mergeState = merger.merge();
    r1.close();
    r2.close();;
    si.setFiles(new HashSet<>(trackingDir.getCreatedFiles()));
      
    if (useCompoundFile) {
      Collection<String> filesToDelete = si.files();
      codec.compoundFormat().write(dir, si, context);
      si.setUseCompoundFile(true);
      for(String name : filesToDelete) {
        si1.info.dir.deleteFile(name);
      }
    }

    return new SegmentCommitInfo(si, 0, -1L, -1L, -1L);
  }


  private void printSegment(PrintWriter out, SegmentCommitInfo si)
    throws Exception {
    SegmentReader reader = new SegmentReader(si, Version.LATEST.major, newIOContext(random()));

    for (int i = 0; i < reader.numDocs(); i++)
      out.println(reader.document(i));

    for (FieldInfo fieldInfo : reader.getFieldInfos()) {
      if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
        continue;
      }
      Terms terms = reader.terms(fieldInfo.name);
      assertNotNull(terms);
      TermsEnum tis = terms.iterator();
      while(tis.next() != null) {

        out.print("  term=" + fieldInfo.name + ":" + tis.term());
        out.println("    DF=" + tis.docFreq());

        PostingsEnum positions = tis.postings(null, PostingsEnum.POSITIONS);

        final Bits liveDocs = reader.getLiveDocs();
        while (positions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          if (liveDocs != null && liveDocs.get(positions.docID()) == false) {
            continue;
          }
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
