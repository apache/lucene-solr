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
import org.apache.lucene.document.Fieldable;
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

  final private DocInverterPerThread perThread;
  final private FieldInfo fieldInfo;
  final InvertedDocConsumerPerField consumer;
  final InvertedDocEndConsumerPerField endConsumer;
  final DocumentsWriter.DocState docState;
  final FieldInvertState fieldState;

  public DocInverterPerField(DocInverterPerThread perThread, FieldInfo fieldInfo) {
    this.perThread = perThread;
    this.fieldInfo = fieldInfo;
    docState = perThread.docState;
    fieldState = perThread.fieldState;
    this.consumer = perThread.consumer.addField(this, fieldInfo);
    this.endConsumer = perThread.endConsumer.addField(this, fieldInfo);
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
  public void processFields(final Fieldable[] fields,
                            final int count) throws IOException {

    fieldState.reset(docState.doc.getBoost());

    final int maxFieldLength = docState.maxFieldLength;

    final boolean doInvert = consumer.start(fields, count);

    for(int i=0;i<count;i++) {

      final Fieldable field = fields[i];

      // TODO FI: this should be "genericized" to querying
      // consumer if it wants to see this particular field
      // tokenized.
      if (field.isIndexed() && doInvert) {
        
        if (i > 0)
          fieldState.position += docState.analyzer == null ? 0 : docState.analyzer.getPositionIncrementGap(fieldInfo.name);

        if (!field.isTokenized()) {		  // un-tokenized field
          String stringValue = field.stringValue();
          final int valueLength = stringValue.length();
          perThread.singleToken.reinit(stringValue, 0, valueLength);
          fieldState.attributeSource = perThread.singleToken;
          consumer.start(field);

          boolean success = false;
          try {
            consumer.add();
            success = true;
          } finally {
            if (!success)
              docState.docWriter.setAborting();
          }
          fieldState.offset += valueLength;
          fieldState.length++;
          fieldState.position++;
        } else {                                  // tokenized field
          final TokenStream stream;
          final TokenStream streamValue = field.tokenStreamValue();

          if (streamValue != null) 
            stream = streamValue;
          else {
            // the field does not have a TokenStream,
            // so we have to obtain one from the analyzer
            final Reader reader;			  // find or make Reader
            final Reader readerValue = field.readerValue();

            if (readerValue != null)
              reader = readerValue;
            else {
              String stringValue = field.stringValue();
              if (stringValue == null)
                throw new IllegalArgumentException("field must have either TokenStream, String or Reader value");
              perThread.stringReader.init(stringValue);
              reader = perThread.stringReader;
            }
          
            // Tokenize field and add to postingTable
            stream = docState.analyzer.reusableTokenStream(fieldInfo.name, reader);
          }

          // reset the TokenStream to the first token
          stream.reset();

          final int startLength = fieldState.length;
          
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
                if (!success)
                  docState.docWriter.setAborting();
              }
              fieldState.position++;
              if (++fieldState.length >= maxFieldLength) {
                if (docState.infoStream != null)
                  docState.infoStream.println("maxFieldLength " +maxFieldLength+ " reached for field " + fieldInfo.name + ", ignoring following tokens");
                break;
              }

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
        fieldState.boost *= field.getBoost();
      }

      // LUCENE-2387: don't hang onto the field, so GC can
      // reclaim
      fields[i] = null;
    }

    consumer.finish();
    endConsumer.finish();
  }
}
