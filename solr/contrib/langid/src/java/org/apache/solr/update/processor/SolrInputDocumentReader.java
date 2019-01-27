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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader on top of SolrInputDocument that can "stream" a document as a character stream in a memory
 * efficient way, to avoid potentially large intermediate string buffers containing whole document content.
 * @lucene.experimental
 */
public class SolrInputDocumentReader extends Reader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrInputDocument doc;
  private final String[] fields;
  private final String fieldValueSep;
  private final int maxTotalChars;
  private final int maxCharsPerFieldValue;
  private int totalCharsConsumed;

  // Remember where we are at
  private int currentFieldIdx = 0;
  private int currentFieldValueIdx = 0;
  private int currentFieldValueOffset = 0;
  private boolean eod = false;
  // Normally a Reader will return -1 at end of document, but to work around LangDetect's bug, we allow another value
  private int eodReturnValue = -1;

  /**
   * Creates a character-stream reader that streams all String fields in the document with space as separator 
   *
   * @param doc Solr input document
   * @param maxCharsPerFieldValue max chars to consume per field value
   * @param maxTotalChars max chars to consume total
   */
  public SolrInputDocumentReader(SolrInputDocument doc, int maxTotalChars, int maxCharsPerFieldValue) {
    this(doc, getStringFields(doc), maxTotalChars, maxCharsPerFieldValue, " ");
  }
  
  /**
   * Creates a character-stream reader that reads the listed fields in order, with
   * max lengths as specified.
   *
   * @param doc Solr input document
   * @param fields list of field names to include
   * @param fieldValueSep separator to insert between field values
   * @param maxCharsPerFieldValue max chars to consume per field value
   * @param maxTotalChars max chars to consume total
   */
  public SolrInputDocumentReader(SolrInputDocument doc, String[] fields, int maxTotalChars,
                                 int maxCharsPerFieldValue, String fieldValueSep) {
    this.doc = doc;
    this.fields = fields;
    this.fieldValueSep = fieldValueSep;
    if (fields == null || fields.length == 0) throw new IllegalArgumentException("fields cannot be empty");
    this.maxTotalChars = maxTotalChars;
    this.maxCharsPerFieldValue = maxCharsPerFieldValue;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    StringBuilder sb = new StringBuilder(len);
    int numChars = fillBuffer(sb, len);

    if (numChars > -1) {
      sb.getChars(0, numChars, cbuf, off);
    }
    totalCharsConsumed += numChars;
    return numChars;
  }

  private int fillBuffer(StringBuilder sb, int targetLen) {
    if (eod) return eodReturnValue;
    if (totalCharsConsumed + targetLen > maxTotalChars) {
      targetLen = maxTotalChars - totalCharsConsumed;
    }

    while (sb.length() < targetLen && !eod) {
      nextDocChunk(sb, targetLen);
    }

    if (sb.length() == 0) {
      eod = true;
      return eodReturnValue;
    } else {
      return sb.length();
    }
  }

  private int nextDocChunk(StringBuilder sb, int maxChunkLength) {
    if (currentFieldIdx > fields.length-1) {
      return returnEod();
    }

    int startFieldValueIdx = currentFieldValueIdx;
    int startFieldValueOffset = currentFieldValueOffset;
    
    do {
      SolrInputField f = doc.getField(fields[currentFieldIdx]);
      if (f == null) {
        log.debug("Field with name {} did not exist on docuemnt.", fields[currentFieldIdx]);
        incField(sb);
        continue;
      }
      Iterator<Object> fvIt = f.iterator();
      currentFieldValueIdx = -1;
      while (fvIt.hasNext() && sb.length() < maxChunkLength) {
        currentFieldValueIdx++;
        String fvStr = String.valueOf(fvIt.next());
        if (currentFieldValueIdx < startFieldValueIdx) continue;
        startFieldValueIdx = 0;
        if (sb.length() > 0) {
          if (maxChunkLength - sb.length() < fieldValueSep.length()) {
            sb.append(fieldValueSep.substring(0,maxChunkLength - sb.length()));
          } else {
            sb.append(fieldValueSep);
          }
        }
        currentFieldValueOffset = startFieldValueOffset;
        startFieldValueOffset = 0;
        int charsNeeded = maxChunkLength - sb.length();
        int endOffset = fvStr.length();
        if (fvStr.length() - currentFieldValueOffset > charsNeeded) {
          endOffset = currentFieldValueOffset + charsNeeded;
        }
        if (endOffset - currentFieldValueOffset > maxCharsPerFieldValue) {
          endOffset = maxCharsPerFieldValue - currentFieldValueOffset;
        }
        sb.append(fvStr.substring(currentFieldValueOffset, endOffset));
        currentFieldValueOffset = endOffset == fvStr.length() ? 0 : endOffset;
      }
      if (sb.length() >= maxChunkLength) {
        return returnValue(sb);
      } else {
        incField(sb);
      }
    } while (currentFieldIdx <= fields.length-1 && sb.length() < maxChunkLength);
    return sb.length() == 0 ? eodReturnValue : sb.length();
  }

  private int returnEod() {
    eod = true;
    return eodReturnValue;
  }

  private int returnValue(StringBuilder sb) {
    if (sb.length() == 0) {
      return returnEod();
    } else {
      return sb.length();
    }
  }

  private void incField(StringBuilder sb) {
    currentFieldIdx++;
    currentFieldValueIdx = 0;
    currentFieldValueOffset = 0;
  }

  @Override
  public void close() throws IOException { /* ignored */ }

  @Override
  public boolean ready() throws IOException {
    return !eod;
  }

  /**
   * Choose another return value than -1 for end of document reached.
   * <b>Warning: Only to work around buggy consumers such as LangDetect 1.1</b>
   * @param eodReturnValue integer which defaults to -1
   */
  public void setEodReturnValue(int eodReturnValue) {
    this.eodReturnValue = eodReturnValue;
  }

  /**
   * Gets the whole reader as a String 
   * @return string of concatenated fields
   */
  public static String asString(Reader reader) {
    try {
      return IOUtils.toString(reader);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed reading doc content from reader", e);
    }
  }
  
  protected static String[] getStringFields(SolrInputDocument doc) {
    Iterable<SolrInputField> iterable = () -> doc.iterator();
        List<String> strFields = StreamSupport.stream(iterable.spliterator(), false)
            .filter(f -> f.getFirstValue() instanceof String)
            .map(SolrInputField::getName).collect(Collectors.toList());
        return strFields.toArray(new String[0]);
  }
}
