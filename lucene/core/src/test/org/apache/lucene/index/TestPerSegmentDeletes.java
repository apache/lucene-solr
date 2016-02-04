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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestPerSegmentDeletes extends LuceneTestCase {
  public void testDeletes1() throws Exception {
    //IndexWriter.debug2 = System.out;
    Directory dir = new MockDirectoryWrapper(new Random(random().nextLong()), new RAMDirectory());
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMaxBufferedDocs(5000);
    iwc.setRAMBufferSizeMB(100);
    RangeMergePolicy fsmp = new RangeMergePolicy(false);
    iwc.setMergePolicy(fsmp);
    IndexWriter writer = new IndexWriter(dir, iwc);
    for (int x = 0; x < 5; x++) {
      writer.addDocument(DocHelper.createDocument(x, "1", 2));
      //System.out.println("numRamDocs(" + x + ")" + writer.numRamDocs());
    }
    //System.out.println("commit1");
    writer.commit();
    assertEquals(1, writer.segmentInfos.size());
    for (int x = 5; x < 10; x++) {
      writer.addDocument(DocHelper.createDocument(x, "2", 2));
      //System.out.println("numRamDocs(" + x + ")" + writer.numRamDocs());
    }
    //System.out.println("commit2");
    writer.commit();
    assertEquals(2, writer.segmentInfos.size());

    for (int x = 10; x < 15; x++) {
      writer.addDocument(DocHelper.createDocument(x, "3", 2));
      //System.out.println("numRamDocs(" + x + ")" + writer.numRamDocs());
    }

    writer.deleteDocuments(new Term("id", "1"));

    writer.deleteDocuments(new Term("id", "11"));

    // flushing without applying deletes means
    // there will still be deletes in the segment infos
    writer.flush(false, false);
    assertTrue(writer.bufferedUpdatesStream.any());

    // get reader flushes pending deletes
    // so there should not be anymore
    IndexReader r1 = writer.getReader();
    assertFalse(writer.bufferedUpdatesStream.any());
    r1.close();

    // delete id:2 from the first segment
    // merge segments 0 and 1
    // which should apply the delete id:2
    writer.deleteDocuments(new Term("id", "2"));
    writer.flush(false, false);
    fsmp = (RangeMergePolicy) writer.getConfig().getMergePolicy();
    fsmp.doMerge = true;
    fsmp.start = 0;
    fsmp.length = 2;
    writer.maybeMerge();

    assertEquals(2, writer.segmentInfos.size());

    // id:2 shouldn't exist anymore because
    // it's been applied in the merge and now it's gone
    IndexReader r2 = writer.getReader();
    int[] id2docs = toDocsArray(new Term("id", "2"), null, r2);
    assertTrue(id2docs == null);
    r2.close();

    /**
    // added docs are in the ram buffer
    for (int x = 15; x < 20; x++) {
      writer.addDocument(TestIndexWriterReader.createDocument(x, "4", 2));
      System.out.println("numRamDocs(" + x + ")" + writer.numRamDocs());
    }
    assertTrue(writer.numRamDocs() > 0);
    // delete from the ram buffer
    writer.deleteDocuments(new Term("id", Integer.toString(13)));

    Term id3 = new Term("id", Integer.toString(3));

    // delete from the 1st segment
    writer.deleteDocuments(id3);

    assertTrue(writer.numRamDocs() > 0);

    //System.out
    //    .println("segdels1:" + writer.docWriter.deletesToString());

    //assertTrue(writer.docWriter.segmentDeletes.size() > 0);

    // we cause a merge to happen
    fsmp.doMerge = true;
    fsmp.start = 0;
    fsmp.length = 2;
    System.out.println("maybeMerge "+writer.segmentInfos);

    SegmentInfo info0 = writer.segmentInfos.info(0);
    SegmentInfo info1 = writer.segmentInfos.info(1);

    writer.maybeMerge();
    System.out.println("maybeMerge after "+writer.segmentInfos);
    // there should be docs in RAM
    assertTrue(writer.numRamDocs() > 0);

    // assert we've merged the 1 and 2 segments
    // and still have a segment leftover == 2
    assertEquals(2, writer.segmentInfos.size());
    assertFalse(segThere(info0, writer.segmentInfos));
    assertFalse(segThere(info1, writer.segmentInfos));

    //System.out.println("segdels2:" + writer.docWriter.deletesToString());

    //assertTrue(writer.docWriter.segmentDeletes.size() > 0);

    IndexReader r = writer.getReader();
    IndexReader r1 = r.getSequentialSubReaders()[0];
    printDelDocs(r1.getLiveDocs());
    int[] docs = toDocsArray(id3, null, r);
    System.out.println("id3 docs:"+Arrays.toString(docs));
    // there shouldn't be any docs for id:3
    assertTrue(docs == null);
    r.close();

    part2(writer, fsmp);
    **/
    // System.out.println("segdels2:"+writer.docWriter.segmentDeletes.toString());
    //System.out.println("close");
    writer.close();
    dir.close();
  }

  /**
  static boolean hasPendingDeletes(SegmentInfos infos) {
    for (SegmentInfo info : infos) {
      if (info.deletes.any()) {
        return true;
      }
    }
    return false;
  }
  **/
  void part2(IndexWriter writer, RangeMergePolicy fsmp) throws Exception {
    for (int x = 20; x < 25; x++) {
      writer.addDocument(DocHelper.createDocument(x, "5", 2));
      //System.out.println("numRamDocs(" + x + ")" + writer.numRamDocs());
    }
    writer.flush(false, false);
    for (int x = 25; x < 30; x++) {
      writer.addDocument(DocHelper.createDocument(x, "5", 2));
      //System.out.println("numRamDocs(" + x + ")" + writer.numRamDocs());
    }
    writer.flush(false, false);

    //System.out.println("infos3:"+writer.segmentInfos);

    Term delterm = new Term("id", "8");
    writer.deleteDocuments(delterm);
    //System.out.println("segdels3:" + writer.docWriter.deletesToString());

    fsmp.doMerge = true;
    fsmp.start = 1;
    fsmp.length = 2;
    writer.maybeMerge();

    // deletes for info1, the newly created segment from the
    // merge should have no deletes because they were applied in
    // the merge
    //SegmentInfo info1 = writer.segmentInfos.info(1);
    //assertFalse(exists(info1, writer.docWriter.segmentDeletes));

    //System.out.println("infos4:"+writer.segmentInfos);
    //System.out.println("segdels4:" + writer.docWriter.deletesToString());
  }

  boolean segThere(SegmentCommitInfo info, SegmentInfos infos) {
    for (SegmentCommitInfo si : infos) {
      if (si.info.name.equals(info.info.name)) return true;
    }
    return false;
  }

  public static void printDelDocs(Bits bits) {
    if (bits == null) return;
    for (int x = 0; x < bits.length(); x++) {
      System.out.println(x + ":" + bits.get(x));
    }
  }

  public int[] toDocsArray(Term term, Bits bits, IndexReader reader)
      throws IOException {
    Fields fields = MultiFields.getFields(reader);
    Terms cterms = fields.terms(term.field);
    TermsEnum ctermsEnum = cterms.iterator();
    if (ctermsEnum.seekExact(new BytesRef(term.text()))) {
      PostingsEnum postingsEnum = TestUtil.docs(random(), ctermsEnum, null, PostingsEnum.NONE);
      return toArray(postingsEnum);
    }
    return null;
  }

  public static int[] toArray(PostingsEnum postingsEnum) throws IOException {
    List<Integer> docs = new ArrayList<>();
    while (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int docID = postingsEnum.docID();
      docs.add(docID);
    }
    return ArrayUtil.toIntArray(docs);
  }

  public class RangeMergePolicy extends MergePolicy {
    boolean doMerge = false;
    int start;
    int length;

    private final boolean useCompoundFile;

    private RangeMergePolicy(boolean useCompoundFile) {
      this.useCompoundFile = useCompoundFile;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer)
        throws IOException {
      MergeSpecification ms = new MergeSpecification();
      if (doMerge) {
        OneMerge om = new OneMerge(segmentInfos.asList().subList(start, start + length));
        ms.add(om);
        doMerge = false;
        return ms;
      }
      return null;
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
        int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
        throws IOException {
      return null;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(
        SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
      return null;
    }

    @Override
    public boolean useCompoundFile(SegmentInfos segments, SegmentCommitInfo newSegment, IndexWriter writer) {
      return useCompoundFile;
    }
  }
}
