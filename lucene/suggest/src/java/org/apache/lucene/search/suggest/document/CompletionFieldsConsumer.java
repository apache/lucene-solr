package org.apache.lucene.search.suggest.document;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.search.suggest.document.CompletionPostingsFormat.CODEC_NAME;
import static org.apache.lucene.search.suggest.document.CompletionPostingsFormat.COMPLETION_VERSION_CURRENT;
import static org.apache.lucene.search.suggest.document.CompletionPostingsFormat.DICT_EXTENSION;
import static org.apache.lucene.search.suggest.document.CompletionPostingsFormat.INDEX_EXTENSION;

/**
 * <p>
 * Weighted FSTs for any indexed {@link SuggestField} is built on {@link #write(Fields)}.
 * A weighted FST maps the analyzed forms of a field to its
 * surface form and document id. FSTs are stored in the CompletionDictionary (.lkp).
 * </p>
 * <p>
 * The file offsets of a field's FST are stored in the CompletionIndex (.cmp)
 * along with the field's internal number {@link FieldInfo#number} on {@link #close()}.
 * </p>
 *
 */
final class CompletionFieldsConsumer extends FieldsConsumer {

  private final String delegatePostingsFormatName;
  private final Map<String, Long> seenFields = new HashMap<>();
  private final SegmentWriteState state;
  private IndexOutput dictOut;
  private FieldsConsumer delegateFieldsConsumer;

  CompletionFieldsConsumer(PostingsFormat delegatePostingsFormat, SegmentWriteState state) throws IOException {
    this.delegatePostingsFormatName = delegatePostingsFormat.getName();
    this.state = state;
    String dictFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, DICT_EXTENSION);
    boolean success = false;
    try {
      this.delegateFieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
      dictOut = state.directory.createOutput(dictFile, state.context);
      CodecUtil.writeIndexHeader(dictOut, CODEC_NAME, COMPLETION_VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(dictOut, delegateFieldsConsumer);
      }
    }
  }

  @Override
  public void write(Fields fields) throws IOException {
    delegateFieldsConsumer.write(fields);

    for (String field : fields) {
      CompletionTermWriter termWriter = new CompletionTermWriter();
      Terms terms = fields.terms(field);
      TermsEnum termsEnum = terms.iterator(null);

      // write terms
      BytesRef term;
      while ((term = termsEnum.next()) != null) {
        termWriter.write(term, termsEnum);
      }

      // store lookup, if needed
      long filePointer = dictOut.getFilePointer();
      if (termWriter.finish(dictOut)) {
        seenFields.put(field, filePointer);
      }
    }
  }

  private boolean closed = false;

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    String indexFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INDEX_EXTENSION);
    boolean success = false;
    try (IndexOutput indexOut = state.directory.createOutput(indexFile, state.context)) {
      delegateFieldsConsumer.close();
      CodecUtil.writeIndexHeader(indexOut, CODEC_NAME, COMPLETION_VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      /*
       * we write the delegate postings format name so we can load it
       * without getting an instance in the ctor
       */
      indexOut.writeString(delegatePostingsFormatName);
      // write # of seen fields
      indexOut.writeVInt(seenFields.size());
      // write field numbers and dictOut offsets
      for (Map.Entry<String, Long> seenField : seenFields.entrySet()) {
        FieldInfo fieldInfo = state.fieldInfos.fieldInfo(seenField.getKey());
        indexOut.writeVInt(fieldInfo.number);
        indexOut.writeVLong(seenField.getValue());
      }
      CodecUtil.writeFooter(indexOut);
      CodecUtil.writeFooter(dictOut);
      IOUtils.close(dictOut);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(dictOut, delegateFieldsConsumer);
      }
    }
  }

  // builds an FST based on the terms written
  private static class CompletionTermWriter {

    private PostingsEnum postingsEnum = null;
    private int docCount = 0;

    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final NRTSuggesterBuilder builder;

    public CompletionTermWriter() {
      builder = new NRTSuggesterBuilder();
    }

    /**
     * Stores the built FST in <code>output</code>
     * Returns true if there was anything stored, false otherwise
     */
    public boolean finish(IndexOutput output) throws IOException {
      boolean stored = builder.store(output);
      assert stored || docCount == 0 : "the FST is null but docCount is != 0 actual value: [" + docCount + "]";
      return stored;
    }

    /**
     * Writes all postings (surface form, weight, document id) for <code>term</code>
     */
    public void write(BytesRef term, TermsEnum termsEnum) throws IOException {
      postingsEnum = termsEnum.postings(null, postingsEnum, PostingsEnum.PAYLOADS);
      builder.startTerm(term);
      int docFreq = 0;
      while (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        int docID = postingsEnum.docID();
        for (int i = 0; i < postingsEnum.freq(); i++) {
          postingsEnum.nextPosition();
          assert postingsEnum.getPayload() != null;
          BytesRef payload = postingsEnum.getPayload();
          ByteArrayDataInput input = new ByteArrayDataInput(payload.bytes, payload.offset, payload.length);
          int len = input.readVInt();
          scratch.grow(len);
          scratch.setLength(len);
          input.readBytes(scratch.bytes(), 0, scratch.length());
          builder.addEntry(docID, scratch.get(), input.readVLong() - 1);
        }
        docFreq++;
        docCount = Math.max(docCount, docFreq + 1);
      }
      builder.finishTerm();
    }
  }
}
