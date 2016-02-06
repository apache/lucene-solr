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
package org.apache.lucene.codecs.simpletext;


import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

/**
 * Writes plain-text stored fields.
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextStoredFieldsWriter extends StoredFieldsWriter {
  private int numDocsWritten = 0;
  private final Directory directory;
  private final String segment;
  private IndexOutput out;
  
  final static String FIELDS_EXTENSION = "fld";
  
  final static BytesRef TYPE_STRING = new BytesRef("string");
  final static BytesRef TYPE_BINARY = new BytesRef("binary");
  final static BytesRef TYPE_INT    = new BytesRef("int");
  final static BytesRef TYPE_LONG   = new BytesRef("long");
  final static BytesRef TYPE_FLOAT  = new BytesRef("float");
  final static BytesRef TYPE_DOUBLE = new BytesRef("double");

  final static BytesRef END      = new BytesRef("END");
  final static BytesRef DOC      = new BytesRef("doc ");
  final static BytesRef FIELD    = new BytesRef("  field ");
  final static BytesRef NAME     = new BytesRef("    name ");
  final static BytesRef TYPE     = new BytesRef("    type ");
  final static BytesRef VALUE    = new BytesRef("    value ");
  
  private final BytesRefBuilder scratch = new BytesRefBuilder();
  
  public SimpleTextStoredFieldsWriter(Directory directory, String segment, IOContext context) throws IOException {
    this.directory = directory;
    this.segment = segment;
    boolean success = false;
    try {
      out = directory.createOutput(IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION), context);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void startDocument() throws IOException {
    write(DOC);
    write(Integer.toString(numDocsWritten));
    newLine();
    
    numDocsWritten++;
  }

  @Override
  public void writeField(FieldInfo info, IndexableField field) throws IOException {
    write(FIELD);
    write(Integer.toString(info.number));
    newLine();
    
    write(NAME);
    write(field.name());
    newLine();
    
    write(TYPE);
    final Number n = field.numericValue();

    if (n != null) {
      if (n instanceof Byte || n instanceof Short || n instanceof Integer) {
        write(TYPE_INT);
        newLine();
          
        write(VALUE);
        write(Integer.toString(n.intValue()));
        newLine();
      } else if (n instanceof Long) {
        write(TYPE_LONG);
        newLine();

        write(VALUE);
        write(Long.toString(n.longValue()));
        newLine();
      } else if (n instanceof Float) {
        write(TYPE_FLOAT);
        newLine();
          
        write(VALUE);
        write(Float.toString(n.floatValue()));
        newLine();
      } else if (n instanceof Double) {
        write(TYPE_DOUBLE);
        newLine();
          
        write(VALUE);
        write(Double.toString(n.doubleValue()));
        newLine();
      } else {
        throw new IllegalArgumentException("cannot store numeric type " + n.getClass());
      }
    } else { 
      BytesRef bytes = field.binaryValue();
      if (bytes != null) {
        write(TYPE_BINARY);
        newLine();
        
        write(VALUE);
        write(bytes);
        newLine();
      } else if (field.stringValue() == null) {
        throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
      } else {
        write(TYPE_STRING);
        newLine();
        
        write(VALUE);
        write(field.stringValue());
        newLine();
      }
    }
  }

  @Override
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (numDocsWritten != numDocs) {
      throw new RuntimeException("mergeFields produced an invalid result: docCount is " + numDocs 
          + " but only saw " + numDocsWritten + " file=" + out.toString() + "; now aborting this merge to prevent index corruption");
    }
    write(END);
    newLine();
    SimpleTextUtil.writeChecksum(out, scratch);
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(out);
    } finally {
      out = null;
    }
  }
  
  private void write(String s) throws IOException {
    SimpleTextUtil.write(out, s, scratch);
  }
  
  private void write(BytesRef bytes) throws IOException {
    SimpleTextUtil.write(out, bytes);
  }
  
  private void newLine() throws IOException {
    SimpleTextUtil.writeNewline(out);
  }
}
