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

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Holds state for inverting all occurrences of a single
 * field in the document.  This class doesn't do anything
 * itself; instead, it forwards the tokens produced by
 * analysis to its own consumer
 * (InvertedDocConsumerPerField).  It also interacts with an
 * endConsumer (InvertedDocEndConsumerPerField).
 */

final class DocInverterPerField extends DocFieldConsumerPerField {

  final private DocInverter parent;
  final FieldInfo fieldInfo;
  final InvertedDocConsumerPerField consumer;
  final InvertedDocEndConsumerPerField endConsumer;
  final DocumentsWriterPerThread.DocState docState;
  final FieldInvertState fieldState;

  public DocInverterPerField(DocInverter parent, FieldInfo fieldInfo) {
    this.parent = parent;
    this.fieldInfo = fieldInfo;
    docState = parent.docState;
    fieldState = parent.fieldState;
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

      // TODO FI: this should be "genericized" to querying
      // consumer if it wants to see this particular field
      // tokenized.
      if (field.fieldType().indexed() && doInvert) {
        
        if (i > 0)
          fieldState.position += docState.analyzer == null ? 0 : docState.analyzer.getPositionIncrementGap(fieldInfo.name);

        // TODO (LUCENE-2309): this analysis logic should be
        // outside of indexer -- field should simply give us
        // a TokenStream, even for multi-valued fields

        if (!field.fieldType().tokenized()) {		  // un-tokenized field
          final String stringValue = field.stringValue();
          assert stringValue != null;
          final int valueLength = stringValue.length();
          parent.singleToken.reinit(stringValue, 0, valueLength);
          fieldState.attributeSource = parent.singleToken;
          consumer.start(field);

          boolean success = false;
          try {
            consumer.add();
            success = true;
          } finally {
            if (!success) {
              docState.docWriter.setAborting();
            }
          }
          fieldState.offset += valueLength;
          fieldState.length++;
          fieldState.position++;
        } else {                                  // tokenized field
          final TokenStream stream;
          final TokenStream streamValue = field.tokenStreamValue();

          if (streamValue != null) {
            stream = streamValue;
          } else {
            // the field does not have a TokenStream,
            // so we have to obtain one from the analyzer
            final Reader reader;			  // find or make Reader
            final Reader readerValue = field.readerValue();

            if (readerValue != null) {
              reader = readerValue;
            } else {
              String stringValue = field.stringValue();
              if (stringValue == null) {
                throw new IllegalArgumentException("field must have either TokenStream, String or Reader value");
              }
              parent.stringReader.init(stringValue);
              reader = parent.stringReader;
            }
          
            // Tokenize field and add to postingTable
            stream = docState.analyzer.reusableTokenStream(fieldInfo.name, reader);
          }

          // reset the TokenStream to the first token
          stream.reset();
          
          try {
            boolean hasMoreTokens = stream.incrementToken();

            fieldState.attributeSource = stream;

            OffsetAttribute offsetAttribute = fieldState.attributeSource.addAttribute(OffsetAttribute.class);
            PositionIncrementAttribute posIncrAttribute = fieldState.attributeSource.addAttribute(PositionIncrementAttribute.class);
            
            consumer.start(field);
            
            for(;;) {

              // If we hit an exception in stream.next below
              // (which is fairly common, eg if analyzer
              // chokes on a given document), then it's
              // non-aborting and (above) this one document
              // will be marked as deleted, but still
              // consume a docID
              
              if (!hasMoreTokens) break;
              
              final int posIncr = posIncrAttribute.getPositionIncrement();
              fieldState.position += posIncr;
              if (fieldState.position > 0) {
                fieldState.position--;
              }

              if (posIncr == 0)
                fieldState.numOverlap++;

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

              hasMoreTokens = stream.incrementToken();
            }
            // trigger streams to perform end-of-stream operations
            stream.end();
            
            fieldState.offset += offsetAttribute.endOffset();
          } finally {
            stream.close();
          }
        }

        fieldState.offset += docState.analyzer == null ? 0 : docState.analyzer.getOffsetGap(field);
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
