package org.apache.lucene.codecs.perfield;

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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader; // javadocs
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;

/**
 * Enables per field format support.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is 
 * written into the index. In order for the field to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} to resolve format names.
 * <p>
 * @see ServiceLoader
 * @lucene.experimental
 */

public abstract class PerFieldPostingsFormat extends PostingsFormat {

  public static final String PER_FIELD_EXTENSION = "per";
  public static final String PER_FIELD_NAME = "PerField40";

  public static final int VERSION_START = 0;
  public static final int VERSION_LATEST = VERSION_START;

  public PerFieldPostingsFormat() {
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
    final String segmentSuffix;

    public FieldsConsumerAndID(FieldsConsumer fieldsConsumer, String segmentSuffix) {
      this.fieldsConsumer = fieldsConsumer;
      this.segmentSuffix = segmentSuffix;
    }

    @Override
    public void close() throws IOException {
      fieldsConsumer.close();
    }
  };
    
  private class FieldsWriter extends FieldsConsumer {

    private final Map<PostingsFormat,FieldsConsumerAndID> formats = new IdentityHashMap<PostingsFormat,FieldsConsumerAndID>();

    /** Records all fields we wrote. */
    private final Map<String,PostingsFormat> fieldToFormat = new HashMap<String,PostingsFormat>();

    private final SegmentWriteState segmentWriteState;

    public FieldsWriter(SegmentWriteState state) throws IOException {
      segmentWriteState = state;
    }

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      final PostingsFormat format = getPostingsFormatForField(field.name);
      if (format == null) {
        throw new IllegalStateException("invalid null PostingsFormat for field=\"" + field.name + "\"");
      }

      assert !fieldToFormat.containsKey(field.name);
      fieldToFormat.put(field.name, format);

      FieldsConsumerAndID consumerAndId = formats.get(format);
      if (consumerAndId == null) {
        // First time we are seeing this format; assign
        // next id and init it:
        final String segmentSuffix = getFullSegmentSuffix(field.name,
                                                          segmentWriteState.segmentSuffix,
                                                          ""+formats.size());
        consumerAndId = new FieldsConsumerAndID(format.fieldsConsumer(new SegmentWriteState(segmentWriteState, segmentSuffix)),
                                                segmentSuffix);
        formats.put(format, consumerAndId);
      }

      return consumerAndId.fieldsConsumer.addField(field);
    }

    @Override
    public void close() throws IOException {

      // Close all subs
      IOUtils.close(formats.values());

      // Write _X.per: maps field name -> format name and
      // format name -> format id
      final String mapFileName = IndexFileNames.segmentFileName(segmentWriteState.segmentName, segmentWriteState.segmentSuffix, PER_FIELD_EXTENSION);
      final IndexOutput out = segmentWriteState.directory.createOutput(mapFileName, segmentWriteState.context);
      boolean success = false;
      try {
        CodecUtil.writeHeader(out, PER_FIELD_NAME, VERSION_LATEST);

        // format name -> int id
        out.writeVInt(formats.size());
        for(Map.Entry<PostingsFormat,FieldsConsumerAndID> ent : formats.entrySet()) {
          out.writeString(ent.getValue().segmentSuffix);
          out.writeString(ent.getKey().getName());
        }

        // field name -> format name
        out.writeVInt(fieldToFormat.size());
        for(Map.Entry<String,PostingsFormat> ent : fieldToFormat.entrySet()) {
          out.writeString(ent.getKey());
          out.writeString(ent.getValue().getName());
        }

        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(out);
        } else {
          IOUtils.close(out);
        }
      }
    }
  }

  static String getFullSegmentSuffix(String fieldName, String outerSegmentSuffix, String segmentSuffix) {
    if (outerSegmentSuffix.length() == 0) {
      return segmentSuffix;
    } else {
      // TODO: support embedding; I think it should work but
      // we need a test confirm to confirm
      // return outerSegmentSuffix + "_" + segmentSuffix;
      throw new IllegalStateException("cannot embed PerFieldPostingsFormat inside itself (field \"" + fieldName + "\" returned PerFieldPostingsFormat)");
    }
  }

  private class FieldsReader extends FieldsProducer {

    private final Map<String,FieldsProducer> fields = new TreeMap<String,FieldsProducer>();
    private final Map<PostingsFormat,FieldsProducer> formats = new IdentityHashMap<PostingsFormat,FieldsProducer>();

    public FieldsReader(final SegmentReadState readState) throws IOException {

      // Read _X.per and init each format:
      boolean success = false;
      try {
        new VisitPerFieldFile(readState.dir, readState.segmentInfo.name, readState.segmentSuffix) {
          @Override
          protected void visitOneFormat(String segmentSuffix, PostingsFormat postingsFormat) throws IOException {
            formats.put(postingsFormat, postingsFormat.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
          }

          @Override
          protected void visitOneField(String fieldName, PostingsFormat postingsFormat) throws IOException {
            assert formats.containsKey(postingsFormat);
            fields.put(fieldName, formats.get(postingsFormat));
          }
        };
        success = true;
      } finally {
        if (!success) {
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
      public String next() throws IOException {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }

        return current;
      }

      @Override
      public Terms terms() throws IOException {
        return fields.get(current).terms(current);
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
    public int size() {
      return fields.size();
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
    public VisitPerFieldFile(Directory dir, String segmentName, String outerSegmentSuffix) throws IOException {
      final String mapFileName = IndexFileNames.segmentFileName(segmentName, outerSegmentSuffix, PER_FIELD_EXTENSION);
      final IndexInput in = dir.openInput(mapFileName, IOContext.READONCE);
      boolean success = false;
      try {
        CodecUtil.checkHeader(in, PER_FIELD_NAME, VERSION_START, VERSION_LATEST);

        // Read format name -> format id
        final int formatCount = in.readVInt();
        for(int formatIDX=0;formatIDX<formatCount;formatIDX++) {
          final String segmentSuffix = in.readString();
          final String formatName = in.readString();
          PostingsFormat postingsFormat = PostingsFormat.forName(formatName);
          //System.out.println("do lookup " + formatName + " -> " + postingsFormat);
          if (postingsFormat == null) {
            throw new IllegalStateException("unable to lookup PostingsFormat for name=\"" + formatName + "\": got null");
          }

          // Better be defined, because it was defined
          // during indexing:
          visitOneFormat(segmentSuffix, postingsFormat);
        }

        // Read field name -> format name
        final int fieldCount = in.readVInt();
        for(int fieldIDX=0;fieldIDX<fieldCount;fieldIDX++) {
          final String fieldName = in.readString();
          final String formatName = in.readString();
          visitOneField(fieldName, PostingsFormat.forName(formatName));
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

    // This is called first, for all formats:
    protected abstract void visitOneFormat(String segmentSuffix, PostingsFormat format) throws IOException;

    // ... then this is called, for all fields:
    protected abstract void visitOneField(String fieldName, PostingsFormat format) throws IOException;
  }

  @Override
  public void files(final SegmentInfo info, String segmentSuffix, final Set<String> files) throws IOException {
    final Directory dir = info.dir;

    final String mapFileName = IndexFileNames.segmentFileName(info.name, segmentSuffix, PER_FIELD_EXTENSION);
    files.add(mapFileName);

    try {
      new VisitPerFieldFile(dir, info.name, segmentSuffix) {
        @Override
        protected void visitOneFormat(String segmentSuffix, PostingsFormat format) throws IOException {
          format.files(info, segmentSuffix, files);
        }

        @Override
          protected void visitOneField(String field, PostingsFormat format) {
        }
      };
    } catch (FileNotFoundException fnfe) {
      // TODO: this is somewhat shady... if we can't open
      // the .per file then most likely someone is calling
      // .files() after this segment was deleted, so, they
      // wouldn't be able to do anything with the files even
      // if we could return them, so we don't add any files
      // in this case.
    }
  }

  // NOTE: only called during writing; for reading we read
  // all we need from the index (ie we save the field ->
  // format mapping)
  public abstract PostingsFormat getPostingsFormatForField(String field);
}
