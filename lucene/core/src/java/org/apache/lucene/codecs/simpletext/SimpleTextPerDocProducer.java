package org.apache.lucene.codecs.simpletext;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesConsumer.DOC;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesConsumer.END;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesConsumer.HEADER;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesConsumer.VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesConsumer.VALUE_SIZE;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.DocValuesArraySource;
import org.apache.lucene.codecs.PerDocProducerBase;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.SortedSource;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.PackedInts.Reader;

/**
 * @lucene.experimental
 */
public class SimpleTextPerDocProducer extends PerDocProducerBase {
  protected final TreeMap<String, DocValues> docValues;
  private Comparator<BytesRef> comp;
  private final String segmentSuffix;

  /**
   * Creates a new {@link SimpleTextPerDocProducer} instance and loads all
   * {@link DocValues} instances for this segment and codec.
   */
  public SimpleTextPerDocProducer(SegmentReadState state,
      Comparator<BytesRef> comp, String segmentSuffix) throws IOException {
    this.comp = comp;
    this.segmentSuffix = segmentSuffix;
    if (anyDocValuesFields(state.fieldInfos)) {
      docValues = load(state.fieldInfos, state.segmentInfo.name,
          state.segmentInfo.docCount, state.dir, state.context);
    } else {
      docValues = new TreeMap<String, DocValues>();
    }
  }

  @Override
  protected Map<String, DocValues> docValues() {
    return docValues;
  }

  protected DocValues loadDocValues(int docCount, Directory dir, String id,
      DocValues.Type type, IOContext context) throws IOException {
    return new SimpleTextDocValues(dir, context, type, id, docCount, comp, segmentSuffix);
  }

  @Override
  protected void closeInternal(Collection<? extends Closeable> closeables)
      throws IOException {
    IOUtils.close(closeables);
  }

  private static class SimpleTextDocValues extends DocValues {

    private int docCount;

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        IOUtils.close(input);
      }
    }

    private Type type;
    private Comparator<BytesRef> comp;
    private int valueSize;
    private final IndexInput input;

    public SimpleTextDocValues(Directory dir, IOContext ctx, Type type,
        String id, int docCount, Comparator<BytesRef> comp, String segmentSuffix) throws IOException {
      this.type = type;
      this.docCount = docCount;
      this.comp = comp;
      final String fileName = IndexFileNames.segmentFileName(id, "", segmentSuffix);
      boolean success = false;
      IndexInput in = null;
      try {
        in = dir.openInput(fileName, ctx);
        valueSize = readHeader(in);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(in);
        }
      }
      input = in;

    }

    @Override
    public Source load() throws IOException {
      boolean success = false;
      IndexInput in = (IndexInput) input.clone();
      try {
        Source source = null;
        switch (type) {
        case BYTES_FIXED_DEREF:
        case BYTES_FIXED_SORTED:
        case BYTES_FIXED_STRAIGHT:
        case BYTES_VAR_DEREF:
        case BYTES_VAR_SORTED:
        case BYTES_VAR_STRAIGHT:
          source = read(in, new ValueReader(type, docCount, comp));
          break;
        case FIXED_INTS_16:
        case FIXED_INTS_32:
        case VAR_INTS:
        case FIXED_INTS_64:
        case FIXED_INTS_8:
        case FLOAT_32:
        case FLOAT_64:
          source = read(in, new ValueReader(type, docCount, null));
          break;
        default:
          throw new IllegalArgumentException("unknown type: " + type);
        }
        assert source != null;
        success = true;
        return source;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(in);
        } else {
          IOUtils.close(in);
        }
      }
    }

    private int readHeader(IndexInput in) throws IOException {
      BytesRef scratch = new BytesRef();
      SimpleTextUtil.readLine(in, scratch);
      assert StringHelper.startsWith(scratch, HEADER);
      SimpleTextUtil.readLine(in, scratch);
      assert StringHelper.startsWith(scratch, VALUE_SIZE);
      return Integer.parseInt(readString(scratch.offset + VALUE_SIZE.length,
          scratch));
    }

    private Source read(IndexInput in, ValueReader reader) throws IOException {
      BytesRef scratch = new BytesRef();
      for (int i = 0; i < docCount; i++) {
        SimpleTextUtil.readLine(in, scratch);

        assert StringHelper.startsWith(scratch, DOC) : scratch.utf8ToString();
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch, VALUE);
        reader.fromString(i, scratch, scratch.offset + VALUE.length);
      }
      SimpleTextUtil.readLine(in, scratch);
      assert scratch.equals(END);
      return reader.getSource();
    }

    @Override
    public Source getDirectSource() throws IOException {
      return this.getSource();
    }

    @Override
    public int getValueSize() {
      return valueSize;
    }

    @Override
    public Type getType() {
      return type;
    }

  }

  public static String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset + offset, scratch.length
        - offset, IOUtils.CHARSET_UTF_8);
  }

  private static final class ValueReader {
    private final Type type;
    private byte[] bytes;
    private short[] shorts;
    private int[] ints;
    private long[] longs;
    private float[] floats;
    private double[] doubles;
    private Source source;
    private BytesRefHash hash;
    private BytesRef scratch;

    public ValueReader(Type type, int maxDocs, Comparator<BytesRef> comp) {
      super();
      this.type = type;
      Source docValuesArray = null;
      switch (type) {
      case FIXED_INTS_16:
        shorts = new short[maxDocs];
        docValuesArray = DocValuesArraySource.forType(type)
            .newFromArray(shorts);
        break;
      case FIXED_INTS_32:
        ints = new int[maxDocs];
        docValuesArray = DocValuesArraySource.forType(type).newFromArray(ints);
        break;
      case FIXED_INTS_64:
        longs = new long[maxDocs];
        docValuesArray = DocValuesArraySource.forType(type)
            .newFromArray(longs);
        break;
      case VAR_INTS:
        longs = new long[maxDocs];
        docValuesArray = new VarIntsArraySource(type, longs);
        break;
      case FIXED_INTS_8:
        bytes = new byte[maxDocs];
        docValuesArray = DocValuesArraySource.forType(type).newFromArray(bytes);
        break;
      case FLOAT_32:
        floats = new float[maxDocs];
        docValuesArray = DocValuesArraySource.forType(type)
            .newFromArray(floats);
        break;
      case FLOAT_64:
        doubles = new double[maxDocs];
        docValuesArray = DocValuesArraySource.forType(type).newFromArray(
            doubles);
        break;
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        assert comp != null;
        hash = new BytesRefHash();
        BytesSource bytesSource = new BytesSource(type, comp, maxDocs, hash);
        ints = bytesSource.docIdToEntry;
        source = bytesSource;
        scratch = new BytesRef();
        break;

      }
      if (docValuesArray != null) {
        assert source == null;
        this.source = docValuesArray;
      }
    }

    public void fromString(int ord, BytesRef ref, int offset) {
      switch (type) {
      case FIXED_INTS_16:
        assert shorts != null;
        shorts[ord] = Short.parseShort(readString(offset, ref));
        break;
      case FIXED_INTS_32:
        assert ints != null;
        ints[ord] = Integer.parseInt(readString(offset, ref));
        break;
      case FIXED_INTS_64:
      case VAR_INTS:
        assert longs != null;
        longs[ord] = Long.parseLong(readString(offset, ref));
        break;
      case FIXED_INTS_8:
        assert bytes != null;
        bytes[ord] = (byte) Integer.parseInt(readString(offset, ref));
        break;
      case FLOAT_32:
        assert floats != null;
        floats[ord] = Float.parseFloat(readString(offset, ref));
        break;
      case FLOAT_64:
        assert doubles != null;
        doubles[ord] = Double.parseDouble(readString(offset, ref));
        break;
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        scratch.bytes = ref.bytes;
        scratch.length = ref.length - offset;
        scratch.offset = ref.offset + offset;
        int key = hash.add(scratch);
        ints[ord] = key < 0 ? (-key) - 1 : key;
        break;
      }
    }

    public Source getSource() {
      if (source instanceof BytesSource) {
        ((BytesSource) source).maybeSort();
      }
      return source;
    }
  }

  private static final class BytesSource extends SortedSource {

    private final BytesRefHash hash;
    int[] docIdToEntry;
    int[] sortedEntries;
    int[] adresses;
    private final boolean isSorted;

    protected BytesSource(Type type, Comparator<BytesRef> comp, int maxDoc,
        BytesRefHash hash) {
      super(type, comp);
      docIdToEntry = new int[maxDoc];
      this.hash = hash;
      isSorted = type == Type.BYTES_FIXED_SORTED
          || type == Type.BYTES_VAR_SORTED;
    }

    void maybeSort() {
      if (isSorted) {
        adresses = new int[hash.size()];
        sortedEntries = hash.sort(getComparator());
        for (int i = 0; i < adresses.length; i++) {
          int entry = sortedEntries[i];
          adresses[entry] = i;
        }
      }

    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      if (isSorted) {
        return hash.get(sortedEntries[ord(docID)], ref);
      } else {
        return hash.get(docIdToEntry[docID], ref);
      }
    }

    @Override
    public SortedSource asSortedSource() {
      if (isSorted) {
        return this;
      }
      return null;
    }

    @Override
    public int ord(int docID) {
      assert isSorted;
      try {
        return adresses[docIdToEntry[docID]];
      } catch (Exception e) {

        return 0;
      }
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      assert isSorted;
      return hash.get(sortedEntries[ord], bytesRef);
    }

    @Override
    public Reader getDocToOrd() {
      return null;
    }

    @Override
    public int getValueCount() {
      return hash.size();
    }

  }
  
  private static class VarIntsArraySource extends Source {

    private final long[] array;

    protected VarIntsArraySource(Type type, long[] array) {
      super(type);
      this.array = array;
    }

    @Override
    public long getInt(int docID) {
      return array[docID];
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      DocValuesArraySource.copyLong(ref, getInt(docID));
      return ref;
    }
    
  }

}
