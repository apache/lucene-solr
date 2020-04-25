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


import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * plain text compound format.
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextCompoundFormat extends CompoundFormat {
  
  /** Sole constructor. */
  public SimpleTextCompoundFormat() {
  }

  @Override
  public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
    String dataFile = IndexFileNames.segmentFileName(si.name, "", DATA_EXTENSION);
    final IndexInput in = dir.openInput(dataFile, context);
    
    BytesRefBuilder scratch = new BytesRefBuilder();

    // first get to TOC:
    DecimalFormat df = new DecimalFormat(OFFSETPATTERN, DecimalFormatSymbols.getInstance(Locale.ROOT));
    long pos = in.length() - TABLEPOS.length - OFFSETPATTERN.length() - 1;
    in.seek(pos);
    SimpleTextUtil.readLine(in, scratch);
    assert StringHelper.startsWith(scratch.get(), TABLEPOS);
    long tablePos = -1; 
    try {
      tablePos = df.parse(stripPrefix(scratch, TABLEPOS)).longValue();
    } catch (ParseException e) {
      throw new CorruptIndexException("can't parse CFS trailer, got: " + scratch.get().utf8ToString(), in);
    }
    
    // seek to TOC and read it
    in.seek(tablePos);
    SimpleTextUtil.readLine(in, scratch);
    assert StringHelper.startsWith(scratch.get(), TABLE);
    int numEntries = Integer.parseInt(stripPrefix(scratch, TABLE));
    
    final String fileNames[] = new String[numEntries];
    final long startOffsets[] = new long[numEntries];
    final long endOffsets[] = new long[numEntries];
    
    for (int i = 0; i < numEntries; i++) {
      SimpleTextUtil.readLine(in, scratch);
      assert StringHelper.startsWith(scratch.get(), TABLENAME);
      fileNames[i] = si.name + IndexFileNames.stripSegmentName(stripPrefix(scratch, TABLENAME));

      if (i > 0) {
        // files must be unique and in sorted order
        assert fileNames[i].compareTo(fileNames[i-1]) > 0;
      }

      SimpleTextUtil.readLine(in, scratch);
      assert StringHelper.startsWith(scratch.get(), TABLESTART);
      startOffsets[i] = Long.parseLong(stripPrefix(scratch, TABLESTART));

      SimpleTextUtil.readLine(in, scratch);
      assert StringHelper.startsWith(scratch.get(), TABLEEND);
      endOffsets[i] = Long.parseLong(stripPrefix(scratch, TABLEEND));
    }

    return new CompoundDirectory() {

      private int getIndex(String name) throws IOException {
        int index = Arrays.binarySearch(fileNames, name);
        if (index < 0) {
          throw new FileNotFoundException("No sub-file found (fileName=" + name + " files: " + Arrays.toString(fileNames) + ")");
        }
        return index;
      }

      @Override
      public String[] listAll() throws IOException {
        ensureOpen();
        return fileNames.clone();
      }

      @Override
      public long fileLength(String name) throws IOException {
        ensureOpen();
        int index = getIndex(name);
        return endOffsets[index] - startOffsets[index];
      }

      @Override
      public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        int index = getIndex(name);
        return in.slice(name, startOffsets[index], endOffsets[index] - startOffsets[index]);
      }

      @Override
      public void close() throws IOException {
        in.close();
      }

      @Override
      public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
      }

      @Override
      public void checkIntegrity() throws IOException {
        // No checksums for SimpleText
      }
    };
  }

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
    String dataFile = IndexFileNames.segmentFileName(si.name, "", DATA_EXTENSION);
    
    int numFiles = si.files().size();
    String names[] = si.files().toArray(new String[numFiles]);
    Arrays.sort(names);
    long startOffsets[] = new long[numFiles];
    long endOffsets[] = new long[numFiles];
    
    BytesRefBuilder scratch = new BytesRefBuilder();
    
    try (IndexOutput out = dir.createOutput(dataFile, context)) { 
      for (int i = 0; i < names.length; i++) {
        // write header for file
        SimpleTextUtil.write(out, HEADER);
        SimpleTextUtil.write(out, names[i], scratch);
        SimpleTextUtil.writeNewline(out);
        
        // write bytes for file
        startOffsets[i] = out.getFilePointer();
        try (IndexInput in = dir.openInput(names[i], IOContext.READONCE)) {
          out.copyBytes(in, in.length());
        }
        endOffsets[i] = out.getFilePointer();
      }
      
      long tocPos = out.getFilePointer();
      
      // write CFS table
      SimpleTextUtil.write(out, TABLE);
      SimpleTextUtil.write(out, Integer.toString(numFiles), scratch);
      SimpleTextUtil.writeNewline(out);
     
      for (int i = 0; i < names.length; i++) {
        SimpleTextUtil.write(out, TABLENAME);
        SimpleTextUtil.write(out, names[i], scratch);
        SimpleTextUtil.writeNewline(out);
        
        SimpleTextUtil.write(out, TABLESTART);
        SimpleTextUtil.write(out, Long.toString(startOffsets[i]), scratch);
        SimpleTextUtil.writeNewline(out);

        SimpleTextUtil.write(out, TABLEEND);
        SimpleTextUtil.write(out, Long.toString(endOffsets[i]), scratch);
        SimpleTextUtil.writeNewline(out);
      }
      
      DecimalFormat df = new DecimalFormat(OFFSETPATTERN, DecimalFormatSymbols.getInstance(Locale.ROOT));
      SimpleTextUtil.write(out, TABLEPOS);
      SimpleTextUtil.write(out, df.format(tocPos), scratch);
      SimpleTextUtil.writeNewline(out);
    }
  }
  
  // helper method to strip strip away 'prefix' from 'scratch' and return as String
  private String stripPrefix(BytesRefBuilder scratch, BytesRef prefix) {
    return new String(scratch.bytes(), prefix.length, scratch.length() - prefix.length, StandardCharsets.UTF_8);
  }
  
  /** Extension of compound file */
  static final String DATA_EXTENSION = "scf";
  
  final static BytesRef HEADER  = new BytesRef("cfs entry for: ");
  
  final static BytesRef TABLE =      new BytesRef("table of contents, size: ");
  final static BytesRef TABLENAME =  new BytesRef("  filename: ");
  final static BytesRef TABLESTART = new BytesRef("    start: ");
  final static BytesRef TABLEEND =   new BytesRef("    end: ");
  
  final static BytesRef TABLEPOS = new BytesRef("table of contents begins at offset: ");
  
  final static String OFFSETPATTERN;
  static {
    int numDigits = Long.toString(Long.MAX_VALUE).length();
    char pattern[] = new char[numDigits];
    Arrays.fill(pattern, '0');
    OFFSETPATTERN = new String(pattern);
  }
}
