package org.apache.lucene.codecs.simpletext;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

class SimpleTextFieldsWriter extends FieldsConsumer implements Closeable {
  
  private final IndexOutput out;
  private final BytesRef scratch = new BytesRef(10);
  private final SegmentWriteState writeState;

  final static BytesRef END          = new BytesRef("END");
  final static BytesRef FIELD        = new BytesRef("field ");
  final static BytesRef TERM         = new BytesRef("  term ");
  final static BytesRef DOC          = new BytesRef("    doc ");
  final static BytesRef FREQ         = new BytesRef("      freq ");
  final static BytesRef POS          = new BytesRef("      pos ");
  final static BytesRef START_OFFSET = new BytesRef("      startOffset ");
  final static BytesRef END_OFFSET   = new BytesRef("      endOffset ");
  final static BytesRef PAYLOAD      = new BytesRef("        payload ");

  public SimpleTextFieldsWriter(SegmentWriteState writeState) throws IOException {
    final String fileName = SimpleTextPostingsFormat.getPostingsFileName(writeState.segmentInfo.name, writeState.segmentSuffix);
    out = writeState.directory.createOutput(fileName, writeState.context);
    this.writeState = writeState;
  }

  @Override
  public void write(Fields fields) throws IOException {
    boolean success = false;
    try {
      write(writeState.fieldInfos, fields);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(this);
      } else {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  public void write(FieldInfos fieldInfos, Fields fields) throws IOException {

    // for each field
    for(String field : fields) {
      Terms terms = fields.terms(field);
      if (terms == null) {
        // Annoyingly, this can happen!
        continue;
      }
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

      boolean wroteField = false;

      boolean hasPositions = terms.hasPositions();

      // TODO: shouldn't we add hasFreqs to Terms?
      // then we don't need FieldInfos here?
      boolean hasFreqs = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_ONLY) > 0;
      boolean hasPayloads = fieldInfo.hasPayloads();
      boolean hasOffsets = terms.hasOffsets();

      int flags = 0;
      if (hasPositions) {
        
        if (hasPayloads) {
          flags = flags | DocsAndPositionsEnum.FLAG_PAYLOADS;
        }
        if (hasOffsets) {
          flags = flags | DocsAndPositionsEnum.FLAG_OFFSETS;
        }
      } else {
        if (hasFreqs) {
          flags = flags | DocsEnum.FLAG_FREQS;
        }
      }

      TermsEnum termsEnum = terms.iterator(null);
      DocsAndPositionsEnum posEnum = null;
      DocsEnum docsEnum = null;

      // for each term in field
      while(true) {
        BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }

        if (hasPositions) {
          posEnum = termsEnum.docsAndPositions(null, posEnum, flags);
          docsEnum = posEnum;
        } else {
          docsEnum = termsEnum.docs(null, docsEnum, flags);
        }
        assert docsEnum != null: "termsEnum=" + termsEnum + " hasPos=" + hasPositions + " flags=" + flags;

        boolean wroteTerm = false;

        // for each doc in field+term
        while(true) {
          int doc = docsEnum.nextDoc();
          if (doc == DocsEnum.NO_MORE_DOCS) {
            break;
          }

          if (!wroteTerm) {

            if (!wroteField) {
              // we lazily do this, in case the field had
              // no terms              
              write(FIELD);
              write(field);
              newline();
              wroteField = true;
            }

            // we lazily do this, in case the term had
            // zero docs
            write(TERM);
            write(term);
            newline();
            wroteTerm = true;
          }

          write(DOC);
          write(Integer.toString(doc));
          newline();
          if (hasFreqs) {
            int freq = docsEnum.freq();
            write(FREQ);
            write(Integer.toString(freq));
            newline();

            if (hasPositions) {
              // for assert:
              int lastStartOffset = 0;

              // for each pos in field+term+doc
              for(int i=0;i<freq;i++) {
                int position = posEnum.nextPosition();

                write(POS);
                write(Integer.toString(position));
                newline();

                if (hasOffsets) {
                  int startOffset = posEnum.startOffset();
                  int endOffset = posEnum.endOffset();
                  assert endOffset >= startOffset;
                  assert startOffset >= lastStartOffset: "startOffset=" + startOffset + " lastStartOffset=" + lastStartOffset;
                  lastStartOffset = startOffset;
                  write(START_OFFSET);
                  write(Integer.toString(startOffset));
                  newline();
                  write(END_OFFSET);
                  write(Integer.toString(endOffset));
                  newline();
                }

                BytesRef payload = posEnum.getPayload();

                if (payload != null && payload.length > 0) {
                  assert payload.length != 0;
                  write(PAYLOAD);
                  write(payload);
                  newline();
                }
              }
            }
          }
        }
      }
    }
  }

  private void write(String s) throws IOException {
    SimpleTextUtil.write(out, s, scratch);
  }

  private void write(BytesRef b) throws IOException {
    SimpleTextUtil.write(out, b);
  }

  private void newline() throws IOException {
    SimpleTextUtil.writeNewline(out);
  }

  @Override
  public void close() throws IOException {
    try {
      write(END);
      newline();
    } finally {
      out.close();
    }
  }
}
