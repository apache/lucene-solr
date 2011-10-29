package org.apache.lucene.index.codecs.perfield;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;

// nocommit: should we allow embedding of PerField in
// another?  it won't work now... because each PerField
// thinks it's allowed to start assigning formats from
// 0... if we made formatID a String then it could work
// (each recursion could add _X to its incoming formatID);
// why is it an int now...?

/**
 * Enables per field format support.
 * 
 * @lucene.experimental
 */
// nocommit:
// expose hook to lookup postings format by name
// expose hook to get postingsformat for a field.
// subclasses can deal with how they implement this (e.g. hashmap with default, solr schema, whatever)
// this class can write its own private .per file with the mappings
public abstract class PerFieldPostingsFormat extends PostingsFormat {

  public static final String PER_FIELD_EXTENSION = "per";
  public static final String PER_FIELD_NAME = "PerField";

  public static final int VERSION_START = 0;
  public static final int VERSION_LATEST = VERSION_START;

  public PerFieldPostingsFormat() {
    // nocommit should we allow caller to pass name in!?
    super(PER_FIELD_NAME);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new FieldsWriter(state);
  }

  // NOTE: not private to avoid $accessN at runtime!!
  static class FieldsConsumerAndID implements Closeable {
    final FieldsConsumer fieldsConsumer;
    final int formatID;

    public FieldsConsumerAndID(FieldsConsumer fieldsConsumer, int formatID) {
      this.fieldsConsumer = fieldsConsumer;
      this.formatID = formatID;
    }

    @Override
    public void close() throws IOException {
      fieldsConsumer.close();
    }
  };
    
  private class FieldsWriter extends FieldsConsumer {

    private final Map<String,FieldsConsumerAndID> formats = new HashMap<String,FieldsConsumerAndID>();

    private final SegmentWriteState segmentWriteState;

    public FieldsWriter(SegmentWriteState state) throws IOException {
      segmentWriteState = state;
    }

    // nocommit -- pass formatID down to addField?

    // nocommit -- should PostingsFormat have a name?

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      final String formatName = getPostingsFormatForField(field.name);
      FieldsConsumerAndID format = formats.get(formatName);
      if (format == null) {
        // First time we are seeing this format -- assign
        // next id and init it:
        final int formatID = formats.size();
        PostingsFormat postingsFormat = getPostingsFormat(formatName);
        // nocommit: maybe the int formatID should be
        // separate arg to .fieldsConsumer?  like we do for
        // .files()
        format = new FieldsConsumerAndID(postingsFormat.fieldsConsumer(new SegmentWriteState(segmentWriteState, formatID)),
                                         formatID);
        formats.put(formatName, format);
      }
      return format.fieldsConsumer.addField(field);
    }

    @Override
    public void close() throws IOException {

      // Close all subs
      IOUtils.close(formats.values());

      // Write _X.per:
      final String mapFileName = IndexFileNames.segmentFileName(segmentWriteState.segmentName, segmentWriteState.formatId, PER_FIELD_EXTENSION);
      final IndexOutput out = segmentWriteState.directory.createOutput(mapFileName, segmentWriteState.context);
      boolean success = false;
      try {
        CodecUtil.writeHeader(out, PER_FIELD_NAME, VERSION_LATEST);
        out.writeVInt(formats.size());
        for(Map.Entry<String,FieldsConsumerAndID> ent : formats.entrySet()) {
          out.writeVInt(ent.getValue().formatID);
          out.writeString(ent.getKey());
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(formats.values());
        } else {
          IOUtils.close(out);
        }
      }
    }
  }

  private class FieldsReader extends FieldsProducer {

    private final Map<String,FieldsProducer> fields = new TreeMap<String,FieldsProducer>();
    private final Map<String,FieldsProducer> formats = new HashMap<String,FieldsProducer>();

    public FieldsReader(final SegmentReadState readState) throws IOException {

      // Read _X.per and init each format:
      boolean success = false;
      try {
        new VisitPerFieldFile(readState.dir, readState.segmentInfo.name) {
          @Override
          protected void visitOneFormat(String formatName, int formatID, PostingsFormat postingsFormat) throws IOException {
            formats.put(formatName, postingsFormat.fieldsProducer(new SegmentReadState(readState, formatID)));
          }
        };
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(formats.values());
        }
      }

      // Map each field to its producer:
      success = false;
      try {
        for (FieldInfo fi : readState.fieldInfos) {
          if (fi.isIndexed) { 
            String formatName = getPostingsFormatForField(fi.name);
            FieldsProducer fieldsProducer = formats.get(formatName);
            // Better be defined, because it was defined
            // during indexing:
            assert fieldsProducer != null;
            fields.put(fi.name, fieldsProducer);
          }
        }
        success = true;
      } finally {
        if (!success) {
          // If we hit exception (eg, IOE because writer was
          // committing, or, for any other reason) we must
          // go back and close all FieldsProducers we opened:
          IOUtils.closeWhileHandlingException(formats.values());
        }
      }
    }
    

    private final class FieldsIterator extends FieldsEnum {
      private final Iterator<String> it;
      private String current;

      public FieldsIterator() {
        it = fields.keySet().iterator();
      }

      @Override
      public String next() {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }

        return current;
      }

      @Override
      public TermsEnum terms() throws IOException {
        final Terms terms = fields.get(current).terms(current);
        if (terms != null) {
          return terms.iterator();
        } else {
          return TermsEnum.EMPTY;
        }
      }
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return new FieldsIterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      FieldsProducer fieldsProducer = fields.get(field);
      return fieldsProducer == null ? null : fieldsProducer.terms(field);
    }
    
    @Override
    public void close() throws IOException {
      IOUtils.close(formats.values());
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new FieldsReader(state);
  }

  private abstract class VisitPerFieldFile {
    public VisitPerFieldFile(Directory dir, String segmentName) throws IOException {
      // nocommit -- should formatID be a String not int?
      // so we can embed one PFPF in another?  ie just keep
      // appending _N to it...
      final String mapFileName = IndexFileNames.segmentFileName(segmentName, 0, PER_FIELD_EXTENSION);
      final IndexInput in = dir.openInput(mapFileName, IOContext.READONCE);
      boolean success = false;
      try {
        CodecUtil.checkHeader(in, PER_FIELD_NAME, VERSION_START, VERSION_LATEST);
        final int formatCount = in.readVInt();
        for(int formatIDX=0;formatIDX<formatCount;formatIDX++) {
          final int formatID = in.readVInt();
          final String formatName = in.readString();
          PostingsFormat postingsFormat = getPostingsFormat(formatName);
          // Better be defined, because it was defined
          // during indexing:
          assert postingsFormat != null;
          visitOneFormat(formatName, formatID, postingsFormat);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(in);
        } else {
          IOUtils.close(in);
        }
      }
    }

    protected abstract void visitOneFormat(String formatName, int formatID, PostingsFormat format) throws IOException;
  }

  @Override
  public void files(final Directory dir, final SegmentInfo info, int formatId, final Set<String> files)
      throws IOException {

    final String mapFileName = IndexFileNames.segmentFileName(info.name, formatId, PER_FIELD_EXTENSION);
    files.add(mapFileName);

    new VisitPerFieldFile(dir, info.name) {
      @Override
      protected void visitOneFormat(String formatName, int formatID, PostingsFormat format) throws IOException {
        format.files(dir, info, formatID, files);
      }
    };
  }

  // nocommit: do we really need to pass fieldInfo here?
  // sucks for 'outsiders' (like tests!) that want to peep at what format
  // is being used for a field... changed to a String for now.. but lets revisit
  public abstract String getPostingsFormatForField(String field);

  public abstract PostingsFormat getPostingsFormat(String formatName);
}
