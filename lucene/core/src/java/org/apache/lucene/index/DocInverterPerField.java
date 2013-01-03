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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.util.IOUtils;

/**
 * Holds state for inverting all occurrences of a single
 * field in the document.  This class doesn't do anything
 * itself; instead, it forwards the tokens produced by
 * analysis to its own consumer
 * (InvertedDocConsumerPerField).  It also interacts with an
 * endConsumer (InvertedDocEndConsumerPerField).
 */

final class DocInverterPerField extends DocFieldConsumerPerField {

  final FieldInfo fieldInfo;
  final InvertedDocConsumerPerField consumer;
  final InvertedDocEndConsumerPerField endConsumer;
  final DocumentsWriterPerThread.DocState docState;
  final FieldInvertState fieldState;

  public DocInverterPerField(DocInverter parent, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    docState = parent.docState;
    fieldState = new FieldInvertState(fieldInfo.name);
    this.consumer = parent.consumer.addField(this, fieldInfo);
    this.endConsumer = parent.endConsumer.addField(this, fieldInfo);
  }

  @Override
  void abort() {
    try {
      consumer.abort();
    } finally {
      endConsumer.abort();
    }
  }

  @Override
  public void processFields(final IndexableField[] fields,
                            final int count) throws IOException {

    fieldState.reset();

    final boolean doInvert = consumer.start(fields, count);

    for(int i=0;i<count;i++) {

      final IndexableField field = fields[i];
      final IndexableFieldType fieldType = field.fieldType();

      // TODO FI: this should be "genericized" to querying
      // consumer if it wants to see this particular field
      // tokenized.
      if (fieldType.indexed() && doInvert) {
        final boolean analyzed = fieldType.tokenized() && docState.analyzer != null;
        
        // if the field omits norms, the boost cannot be indexed.
        if (fieldType.omitNorms() && field.boost() != 1.0f) {
          throw new UnsupportedOperationException("You cannot set an index-time boost: norms are omitted for field '" + field.name() + "'");
        }
        
        // only bother checking offsets if something will consume them.
        // TODO: after we fix analyzers, also check if termVectorOffsets will be indexed.
        final boolean checkOffsets = fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        int lastStartOffset = 0;

        if (i > 0) {
          fieldState.position += analyzed ? docState.analyzer.getPositionIncrementGap(fieldInfo.name) : 0;
        }

        final TokenStream stream = field.tokenStream(docState.analyzer);
        // reset the TokenStream to the first token
        stream.reset();

        boolean success2 = false;

        try {
          boolean hasMoreTokens = stream.incrementToken();

          fieldState.attributeSource = stream;

          OffsetAttribute offsetAttribute = fieldState.attributeSource.addAttribute(OffsetAttribute.class);
          PositionIncrementAttribute posIncrAttribute = fieldState.attributeSource.addAttribute(PositionIncrementAttribute.class);

          if (hasMoreTokens) {
            consumer.start(field);

            do {
              // If we hit an exception in stream.next below
              // (which is fairly common, eg if analyzer
              // chokes on a given document), then it's
              // non-aborting and (above) this one document
              // will be marked as deleted, but still
              // consume a docID

              final int posIncr = posIncrAttribute.getPositionIncrement();
              if (posIncr < 0) {
                throw new IllegalArgumentException("position increment must be >=0 (got " + posIncr + ")");
              }
              if (fieldState.position == 0 && posIncr == 0) {
                throw new IllegalArgumentException("first position increment must be > 0 (got 0)");
              }
              int position = fieldState.position + posIncr;
              if (position > 0) {
                // NOTE: confusing: this "mirrors" the
                // position++ we do below
                position--;
              } else if (position < 0) {
                throw new IllegalArgumentException("position overflow for field '" + field.name() + "'");
              }
              
              // position is legal, we can safely place it in fieldState now.
              // not sure if anything will use fieldState after non-aborting exc...
              fieldState.position = position;

              if (posIncr == 0)
                fieldState.numOverlap++;
              
              if (checkOffsets) {
                int startOffset = fieldState.offset + offsetAttribute.startOffset();
                int endOffset = fieldState.offset + offsetAttribute.endOffset();
                if (startOffset < 0 || endOffset < startOffset) {
                  throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, "
                      + "startOffset=" + startOffset + ",endOffset=" + endOffset);
                }
                if (startOffset < lastStartOffset) {
                  throw new IllegalArgumentException("offsets must not go backwards startOffset=" 
                       + startOffset + " is < lastStartOffset=" + lastStartOffset);
                }
                lastStartOffset = startOffset;
              }

              boolean success = false;
              try {
                // If we hit an exception in here, we abort
                // all buffered documents since the last
                // flush, on the likelihood that the
                // internal state of the consumer is now
                // corrupt and should not be flushed to a
                // new segment:
                consumer.add();
                success = true;
              } finally {
                if (!success) {
                  docState.docWriter.setAborting();
                }
              }
              fieldState.length++;
              fieldState.position++;
            } while (stream.incrementToken());
          }
          // trigger streams to perform end-of-stream operations
          stream.end();

          fieldState.offset += offsetAttribute.endOffset();
          success2 = true;
        } finally {
          if (!success2) {
            IOUtils.closeWhileHandlingException(stream);
          } else {
            stream.close();
          }
        }

        fieldState.offset += analyzed ? docState.analyzer.getOffsetGap(fieldInfo.name) : 0;
        fieldState.boost *= field.boost();
      }

      // LUCENE-2387: don't hang onto the field, so GC can
      // reclaim
      fields[i] = null;
    }

    consumer.finish();
    endConsumer.finish();
  }

  @Override
  FieldInfo getFieldInfo() {
    return fieldInfo;
  }
}
