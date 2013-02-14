package org.apache.lucene.codecs.asserting;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.codecs.lucene40.Lucene40TermVectorsFormat;
import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

/**
 * Just like {@link Lucene40TermVectorsFormat} but with additional asserts.
 */
public class AssertingTermVectorsFormat extends TermVectorsFormat {
  private final TermVectorsFormat in = new Lucene40TermVectorsFormat();

  @Override
  public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    return new AssertingTermVectorsReader(in.vectorsReader(directory, segmentInfo, fieldInfos, context));
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    return new AssertingTermVectorsWriter(in.vectorsWriter(directory, segmentInfo, context));
  }

  static class AssertingTermVectorsReader extends TermVectorsReader {
    private final TermVectorsReader in;

    AssertingTermVectorsReader(TermVectorsReader in) {
      this.in = in;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public Fields get(int doc) throws IOException {
      Fields fields = in.get(doc);
      return fields == null ? null : new AssertingAtomicReader.AssertingFields(fields);
    }

    @Override
    public TermVectorsReader clone() {
      return new AssertingTermVectorsReader(in.clone());
    }
  }

  enum Status {
    UNDEFINED, STARTED, FINISHED;
  }

  static class AssertingTermVectorsWriter extends TermVectorsWriter {
    private final TermVectorsWriter in;
    private Status docStatus, fieldStatus, termStatus;
    private int docCount, fieldCount, termCount, positionCount;
    boolean hasPositions;

    AssertingTermVectorsWriter(TermVectorsWriter in) {
      this.in = in;
      docStatus = Status.UNDEFINED;
      fieldStatus = Status.UNDEFINED;
      termStatus = Status.UNDEFINED;
      fieldCount = termCount = positionCount = 0;
    }

    @Override
    public void startDocument(int numVectorFields) throws IOException {
      assert fieldCount == 0;
      assert docStatus != Status.STARTED;
      in.startDocument(numVectorFields);
      docStatus = Status.STARTED;
      fieldCount = numVectorFields;
      docCount++;
    }

    @Override
    public void finishDocument() throws IOException {
      assert fieldCount == 0;
      assert docStatus == Status.STARTED;
      in.finishDocument();
      docStatus = Status.FINISHED;
    }

    @Override
    public void startField(FieldInfo info, int numTerms, boolean positions,
        boolean offsets, boolean payloads) throws IOException {
      assert termCount == 0;
      assert docStatus == Status.STARTED;
      assert fieldStatus != Status.STARTED;
      in.startField(info, numTerms, positions, offsets, payloads);
      fieldStatus = Status.STARTED;
      termCount = numTerms;
      hasPositions = positions || offsets || payloads;
    }

    @Override
    public void finishField() throws IOException {
      assert termCount == 0;
      assert fieldStatus == Status.STARTED;
      in.finishField();
      fieldStatus = Status.FINISHED;
      --fieldCount;
    }

    @Override
    public void startTerm(BytesRef term, int freq) throws IOException {
      assert docStatus == Status.STARTED;
      assert fieldStatus == Status.STARTED;
      assert termStatus != Status.STARTED;
      in.startTerm(term, freq);
      termStatus = Status.STARTED;
      positionCount = hasPositions ? freq : 0;
    }

    @Override
    public void finishTerm() throws IOException {
      assert positionCount == 0;
      assert docStatus == Status.STARTED;
      assert fieldStatus == Status.STARTED;
      assert termStatus == Status.STARTED;
      in.finishTerm();
      termStatus = Status.FINISHED;
      --termCount;
    }

    @Override
    public void addPosition(int position, int startOffset, int endOffset,
        BytesRef payload) throws IOException {
      assert docStatus == Status.STARTED;
      assert fieldStatus == Status.STARTED;
      assert termStatus == Status.STARTED;
      in.addPosition(position, startOffset, endOffset, payload);
      --positionCount;
    }

    @Override
    public void abort() {
      in.abort();
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
      assert docCount == numDocs;
      assert docStatus == (numDocs > 0 ? Status.FINISHED : Status.UNDEFINED);
      assert fieldStatus != Status.STARTED;
      assert termStatus != Status.STARTED;
      in.finish(fis, numDocs);
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return in.getComparator();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

  }
}
