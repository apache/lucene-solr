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
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;


/**
 * plain text doc values format.
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextSimpleDocValuesFormat extends SimpleDocValuesFormat {
  final static BytesRef END     = new BytesRef("END");
  final static BytesRef FIELD   = new BytesRef("field ");
  // used for numerics
  final static BytesRef MINVALUE = new BytesRef("  minvalue ");
  final static BytesRef PATTERN  = new BytesRef("  pattern ");

  @Override
  public SimpleDVConsumer fieldsConsumer(Directory dir, SegmentInfo si, FieldInfos fis, IOContext context) throws IOException {
    return new SimpleTextDocValuesWriter(dir, si, context);
  }
  
  /** the .dat file contains the data.
   *  for numbers this is a "fixed-width" file, for example a single byte range:
   *  <pre>
   *  field myField
   *    minvalue 0
   *    pattern 000
   *  005
   *  234
   *  123
   *  ...
   *  </pre>
   *  so a document's value (delta encoded from minvalue) can be retrieved by 
   *  seeking to startOffset + (1+pattern.length())*docid. The extra 1 is the newline.
   *   
   *  the reader can just scan this file when it opens, skipping over the data blocks
   *  and saving the offset/etc for each field. 
   */
  static class SimpleTextDocValuesWriter extends SimpleDVConsumer {
    final IndexOutput data;
    final BytesRef scratch = new BytesRef();
    
    SimpleTextDocValuesWriter(Directory dir, SegmentInfo si, IOContext context) throws IOException {
      data = dir.createOutput(IndexFileNames.segmentFileName(si.name, "", "dat"), context);
    }

    @Override
    public NumericDocValuesConsumer addNumericField(FieldInfo field, final long minValue, long maxValue) throws IOException {
      writeFieldEntry(field);
      
      // write our minimum value to the .dat, all entries are deltas from that
      SimpleTextUtil.write(data, MINVALUE);
      SimpleTextUtil.write(data, Long.toString(minValue), scratch);
      SimpleTextUtil.writeNewline(data);

      // build up our fixed-width "simple text packed ints" format
      int maxBytesPerValue = Long.toString(maxValue - minValue).length();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < maxBytesPerValue; i++) {
        sb.append('0');
      }
      
      // write our pattern to the .dat
      SimpleTextUtil.write(data, PATTERN);
      SimpleTextUtil.write(data, sb.toString(), scratch);
      SimpleTextUtil.writeNewline(data);
      
      final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));
      return new NumericDocValuesConsumer() {
        int numDocsWritten = 0;

        @Override
        public void add(long value) throws IOException {
          long delta = value - minValue;
          SimpleTextUtil.write(data, encoder.format(delta), scratch);
          SimpleTextUtil.writeNewline(data);
          numDocsWritten++;
        }

        @Override
        public void finish(FieldInfos fieldInfos, int numDocs) throws IOException {
          assert numDocs == numDocsWritten;
          // nocommit: hopefully indexwriter is responsible for "filling" like it does stored fields!
        }
      };
    }

    @Override
    public BinaryDocValuesConsumer addBinaryField(FieldInfo field, boolean fixedLength, int maxLength) throws IOException {
      writeFieldEntry(field);
      return null; // nocommit
    }

    @Override
    public SortedDocValuesConsumer addSortedField(FieldInfo field) throws IOException {
      return null; // nocommit
    }
    
    /** write the header for this field */
    private void writeFieldEntry(FieldInfo field) throws IOException {
      SimpleTextUtil.write(data, FIELD);
      SimpleTextUtil.write(data, field.name, scratch);
      SimpleTextUtil.writeNewline(data);
    }
    
    @Override
    public void close() throws IOException {
      boolean success = false;
      try {
        // TODO: sheisty to do this here?
        SimpleTextUtil.write(data, END);
        SimpleTextUtil.writeNewline(data);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data);
        } else {
          IOUtils.closeWhileHandlingException(data);
        }
      }
    }
  };
  
}
