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

package org.apache.solr.common.util;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.DataEntry.EntryListener;

import static org.apache.solr.common.util.FastJavaBinDecoder.Tag._EXTERN_STRING;
import static org.apache.solr.common.util.JavaBinCodec.*;

public class FastJavaBinDecoder implements DataEntry.FastDecoder {
  private StreamCodec codec;
  private EntryImpl rootEntry = new EntryImpl();
  private InputStream stream;

  private static final DataEntry.EntryListener emptylistener = e -> {
  };


  @Override
  public FastJavaBinDecoder withInputStream(InputStream is) {
    this.stream = is;
    return this;
  }

  @Override
  public Object decode(EntryListener listener) throws IOException {
    rootEntry.entryListener = listener == null ? emptylistener : listener;
    codec = new StreamCodec(stream);
    codec.start();
    EntryImpl entry = codec.beginRead(rootEntry);
    listener.entry(entry);
    if (entry.tag.type.isContainer && entry.entryListener != null) {
      entry.tag.stream(entry, codec);
    }
    return entry.ctx;
  }


  static class StreamCodec extends JavaBinCodec {

    final FastInputStream dis;

    StreamCodec(InputStream is) {
      this.dis = FastInputStream.wrap(is);
    }


    public void skip(int sz) throws IOException {
      while (sz > 0) {
        int read = dis.read(bytes, 0, Math.min(bytes.length, sz));
        sz -= read;
      }

    }


    void start() throws IOException {
      _init(dis);
    }


    Tag getTag() throws IOException {
      tagByte = dis.readByte();
      switch (tagByte >>> 5) {
        case STR >>> 5:
          return Tag._STR;
        case SINT >>> 5:
          return Tag._SINT;
        case SLONG >>> 5:
          return Tag._SLONG;
        case ARR >>> 5:
          return Tag._ARR;
        case ORDERED_MAP >>> 5:
          return Tag._ORDERED_MAP;
        case NAMED_LST >>> 5:
          return Tag._NAMED_LST;
        case EXTERN_STRING >>> 5:
          return _EXTERN_STRING;
      }

      Tag t = lower5BitTags[tagByte];
      if (t == null) throw new RuntimeException("Invalid type " + tagByte);
      return t;
    }

    public ByteBuffer readByteBuffer(DataInputInputStream dis, int sz) throws IOException {
      ByteBuffer result = dis.readDirectByteBuffer(sz);
      if(result != null) return result;
      byte[] arr = new byte[readVInt(dis)];
      dis.readFully(arr);
      return ByteBuffer.wrap(arr);
    }

    public CharSequence readObjKey(Tag ktag) throws IOException {
      CharSequence key = null;
      if (ktag.type == DataEntry.Type.STR) {
        if (ktag == _EXTERN_STRING) key = readExternString(dis);
        else key = readStr(dis);
      } else if (ktag.type == DataEntry.Type.NULL) {
        //no need to do anything
      } else {
        throw new RuntimeException("Key must be String");
      }
      return key;
    }

    public EntryImpl beginRead(EntryImpl parent) throws IOException {
      EntryImpl entry = parent.getChildAndReset();
      entry.tag = getTag();
      entry.tag.lazyRead(entry, this);
      if (entry.tag.type.isPrimitive) entry.consumedFully = true;
      return entry;
    }
  }


  public class EntryImpl implements DataEntry {
    //size
    int size = -1;
    Tag tag;
    Object metadata;
    EntryImpl parent, child;
    long numericVal;
    double doubleVal;
    Object objVal;
    public Object ctx;
    boolean boolVal;
    boolean mapEntry;
    long idx;

    EntryListener entryListener;

    boolean consumedFully = false;
    int depth = 0;
    CharSequence name;


    EntryImpl getChildAndReset() {
      if (child == null) {
        child = new EntryImpl();
        child.parent = this;
        child.depth = depth + 1;
      }
      child.reset();
      return child;

    }

    @Override
    public long index() {
      return idx;
    }

    @Override
    public int length() {
      return size;
    }

    public Tag getTag() {
      return tag;
    }

    @Override
    public boolean boolVal() {
      return boolVal;
    }

    @Override
    public boolean isKeyValEntry() {
      return mapEntry;
    }

    @Override
    public CharSequence name() {
      return name;
    }

    @Override
    public int depth() {
      return depth;
    }

    @Override
    public DataEntry parent() {
      return parent;
    }

    @Override
    public Object metadata() {
      return metadata;
    }

    @Override
    public Object ctx() {
      return parent == null ? null : parent.ctx;
    }

    @Override
    public Type type() {
      return tag.type;
    }

    @Override
    public int intVal() {
      return (int) numericVal;
    }

    @Override
    public long longVal() {
      return numericVal;
    }

    @Override
    public float floatVal() {
      if(tag.type == Type.FLOAT) return (float) doubleVal;
      else {
        return ((Number) val()).floatValue();
      }
    }

    @Override
    public double doubleVal() {
      return doubleVal;
    }

    @Override
    public Object val() {
      if (objVal != null) return objVal;
      try {
        return objVal = tag.readObject(codec, this);
      } catch (IOException e) {
        throw new RuntimeException("Error with stream", e);
      } finally {
        consumedFully = true;
      }
    }

    @Override
    public void listenContainer(Object ctx, EntryListener listener) {
      this.entryListener = listener;
      this.ctx = ctx;
    }

    void reset() {
      this.doubleVal = 0.0d;
      this.numericVal = 0l;
      this.objVal = null;
      this.ctx = null;
      this.entryListener = null;
      this.size = -1;
      this.tag = null;
      consumedFully = false;
      metadata = null;
      name = null;
      idx = -1;
    }

    public void callEnd() {
      if (entryListener != null) entryListener.end(this);

    }
  }

  static final boolean LOWER_5_BITS = true;
  static final boolean UPPER_3_BITS = false;

  public enum Tag {
    _NULL(NULL, LOWER_5_BITS, DataEntry.Type.NULL),
    _BOOL_TRUE(BOOL_TRUE, LOWER_5_BITS, DataEntry.Type.BOOL) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) {
        entry.boolVal = true;
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Boolean.TRUE;
      }
    },
    _BOOL_FALSE(BOOL_FALSE, LOWER_5_BITS, DataEntry.Type.BOOL) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) {
        entry.boolVal = false;
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Boolean.FALSE;
      }
    },
    _BYTE(BYTE, LOWER_5_BITS, DataEntry.Type.INT) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.numericVal = streamCodec.dis.readByte();
        entry.consumedFully = true;
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Byte.valueOf((byte) entry.numericVal);
      }
    },
    _SHORT(SHORT, LOWER_5_BITS, DataEntry.Type.INT) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.numericVal = streamCodec.dis.readShort();
        entry.consumedFully = true;
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Short.valueOf((short) entry.numericVal);
      }
    },
    _DOUBLE(DOUBLE, LOWER_5_BITS, DataEntry.Type.DOUBLE) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.doubleVal = streamCodec.dis.readDouble();
        entry.consumedFully = true;
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Double.valueOf(entry.doubleVal);
      }
    },
    _INT(INT, LOWER_5_BITS, DataEntry.Type.INT) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.numericVal = streamCodec.dis.readInt();
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Integer.valueOf((int) entry.numericVal);
      }

    },//signed integer
    _LONG(LONG, LOWER_5_BITS, DataEntry.Type.LONG) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.numericVal = streamCodec.dis.readLong();
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Long.valueOf(entry.numericVal);
      }
    },
    _FLOAT(FLOAT, LOWER_5_BITS, DataEntry.Type.FLOAT) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.doubleVal = streamCodec.dis.readFloat();
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Float.valueOf((float) entry.doubleVal);
      }
    },
    _DATE(DATE, LOWER_5_BITS, DataEntry.Type.DATE) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec streamCodec) throws IOException {
        entry.numericVal = streamCodec.dis.readLong();
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return new Date(entry.numericVal);
      }
    },
    _MAP(MAP, LOWER_5_BITS, DataEntry.Type.KEYVAL_ITER) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.size = readObjSz(codec, entry.tag);
      }

      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        try {
          for (int i = 0; i < entry.size; i++) {
            CharSequence key = codec.readObjKey(codec.getTag());
            callbackMapEntryListener(entry, key, codec, i);
          }
        } finally {
          entry.callEnd();
        }
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readMap(codec.dis,entry.size);
      }
    },
    _SOLRDOC(SOLRDOC, LOWER_5_BITS, DataEntry.Type.KEYVAL_ITER) {
      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        try {
          codec.getTag();
          entry.size = codec.readSize(codec.dis);//  readObjSz(codec, entry.tag);
          for (int i = 0; i < entry.size; i++) {
            Tag tag = codec.getTag();
            if (tag == _SOLRDOC) {
              EntryImpl e = entry.getChildAndReset();
              e.tag = tag;
              e.idx = i;
              Tag.callbackIterListener(entry, e, codec);
            } else {
              CharSequence key = codec.readObjKey(tag);
              callbackMapEntryListener(entry, key, codec, i);
            }

          }
        } finally {
          entry.callEnd();

        }
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readSolrDocument(codec.dis);
      }
    },
    _SOLRDOCLST(SOLRDOCLST, LOWER_5_BITS, DataEntry.Type.ENTRY_ITER) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.metadata = codec.readVal(codec.dis);
        codec.getTag();//ignore this
        entry.size = codec.readSize(codec.dis);
      }

      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        try {
          for (int i = 0; i < entry.size; i++) {
            EntryImpl newEntry = codec.beginRead(entry);
            newEntry.idx = i;
            Tag.callbackIterListener(entry, newEntry, codec);
          }
        } finally {
          entry.callEnd();
        }
      }

      @Override
      @SuppressWarnings({"unchecked"})
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        SolrDocumentList solrDocs = new SolrDocumentList();
        if(entry.metadata != null){
          @SuppressWarnings({"rawtypes"})
          List list = (List) entry.metadata;
          solrDocs.setNumFound((Long) list.get(0));
          solrDocs.setStart((Long) list.get(1));
          solrDocs.setMaxScore((Float) list.get(2));
          if (list.size() > 3) { //needed for back compatibility
            solrDocs.setNumFoundExact((Boolean)list.get(3));
          }
        }
        List<SolrDocument> l =  codec.readArray(codec.dis, entry.size);
        solrDocs.addAll(l);
        return solrDocs;
      }
    },
    _BYTEARR(BYTEARR, LOWER_5_BITS, DataEntry.Type.BYTEARR) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.size = readVInt(codec.dis);
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        ByteBuffer buf = codec.readByteBuffer(codec.dis, entry.size);
        entry.size = buf.limit() - buf.position();
        return buf;
      }

      @Override
      public void skip(EntryImpl entry, StreamCodec codec) throws IOException {
        codec.skip(entry.size);
      }
    },
    _ITERATOR(ITERATOR, LOWER_5_BITS, DataEntry.Type.ENTRY_ITER) {
      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        try {
          long idx = 0;
          while (true) {
            EntryImpl newEntry = codec.beginRead(entry);
            newEntry.idx = idx++;
            if (newEntry.tag == _END) break;
            newEntry.idx = idx++;
            Tag.callbackIterListener(entry, newEntry, codec);
          }
        } finally {
          entry.callEnd();
        }
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readIterator(codec.dis);
      }
    },

    _END(END, LOWER_5_BITS, null),

    _SOLRINPUTDOC(SOLRINPUTDOC, LOWER_5_BITS, DataEntry.Type.JAVA_OBJ) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.objVal = readObject(codec, entry);
        entry.consumedFully = true;
      }
    },
    _MAP_ENTRY_ITER(MAP_ENTRY_ITER, LOWER_5_BITS, DataEntry.Type.KEYVAL_ITER) {
      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        long idx = 0;
        for (; ; ) {
          Tag tag = codec.getTag();
          if (tag == Tag._END) break;
          CharSequence key = codec.readObjKey(tag);
          callbackMapEntryListener(entry, key, codec, idx++);
        }
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readMapIter(codec.dis);
      }
    },
    _ENUM_FIELD_VALUE(ENUM_FIELD_VALUE, LOWER_5_BITS, DataEntry.Type.JAVA_OBJ) {

      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.objVal =codec.readEnumFieldValue(codec.dis);
        entry.consumedFully = true;
      }
    },
    _MAP_ENTRY(MAP_ENTRY, LOWER_5_BITS, DataEntry.Type.JAVA_OBJ) {
      //doesn't support streaming
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.objVal = codec.readMapEntry(codec.dis);
        entry.consumedFully = true;
      }
    },
    // types that combine tag + length (or other info) in a single byte
    _TAG_AND_LEN(TAG_AND_LEN, UPPER_3_BITS, null),
    _STR(STR, UPPER_3_BITS, DataEntry.Type.STR) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.size = readObjSz(codec, this);

      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readUtf8(codec.dis);
      }

      @Override
      public void skip(EntryImpl entry, StreamCodec codec) throws IOException {
        codec.skip(entry.size);
      }
    },
    _SINT(SINT, UPPER_3_BITS, DataEntry.Type.INT) {//unsigned integer
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.numericVal = codec.readSmallInt(codec.dis);
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Integer.valueOf((int) entry.numericVal);
      }

    },
    _SLONG(SLONG, UPPER_3_BITS, DataEntry.Type.LONG) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.numericVal = codec.readSmallLong(codec.dis);
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) {
        return Long.valueOf((int) entry.numericVal);
      }


    },
    _ARR(ARR, UPPER_3_BITS, DataEntry.Type.ENTRY_ITER) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.size = readObjSz(codec, this);
      }

      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        for (int i = 0; i < entry.size; i++) {
          EntryImpl newEntry = codec.beginRead(entry);
          newEntry.idx = i;
          Tag.callbackIterListener(entry, newEntry, codec);
        }
      }


      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readArray(codec.dis);
      }
    }, //
    _ORDERED_MAP(ORDERED_MAP, UPPER_3_BITS, DataEntry.Type.KEYVAL_ITER) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.size = readObjSz(codec, entry.tag);
      }

      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        _MAP.stream(entry, codec);
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readOrderedMap(codec.dis);
      }

    }, // SimpleOrderedMap (a NamedList subclass, and more common)
    _NAMED_LST(NAMED_LST, UPPER_3_BITS, DataEntry.Type.KEYVAL_ITER) {
      @Override
      public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {
        entry.size = readObjSz(codec, entry.tag);
      }

      @Override
      public void stream(EntryImpl entry, StreamCodec codec) throws IOException {
        _MAP.stream(entry, codec);
      }

      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readNamedList(codec.dis);
      }
    }, // NamedList

    _EXTERN_STRING(EXTERN_STRING, UPPER_3_BITS, DataEntry.Type.STR) {
      @Override
      public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
        return codec.readExternString(codec.dis);
      }
    };

    private static int readObjSz(StreamCodec codec, Tag tag) throws IOException {
      return tag.isLower5Bits ?
          StreamCodec.readVInt(codec.dis) :
          codec.readSize(codec.dis);
    }

    private static void callbackMapEntryListener(EntryImpl entry, CharSequence key, StreamCodec codec, long idx)
        throws IOException {
      EntryImpl newEntry = codec.beginRead(entry);
      newEntry.name = key;
      newEntry.mapEntry = true;
      newEntry.idx = idx;
      try {
        if (entry.entryListener != null) entry.entryListener.entry(newEntry);
      } finally {
        // the listener did not consume the entry
        postCallback(codec, newEntry);
      }
    }

    private static void callbackIterListener(EntryImpl parent, EntryImpl newEntry, StreamCodec codec)
        throws IOException {
      try {
        newEntry.mapEntry = false;
        if(parent.entryListener != null) parent.entryListener.entry(newEntry);
      } finally {
        // the listener did not consume the entry
        postCallback(codec, newEntry);
      }
    }

    private static void postCallback(StreamCodec codec, EntryImpl newEntry) throws IOException {
      if (!newEntry.consumedFully) {
        if (newEntry.tag.type.isContainer) {
          //this is a map like container object and there is a listener
          if (newEntry.entryListener == null) newEntry.entryListener = emptylistener;
          newEntry.tag.stream(newEntry, codec);
        } else {
          newEntry.tag.skip(newEntry, codec);
        }
      }
    }


    final int code;
    final boolean isLower5Bits;
    final DataEntry.Type type;

    Tag(int code, boolean isLower5Bits, DataEntry.Type type) {
      this.code = code;
      this.isLower5Bits = isLower5Bits;
      this.type = type;
    }

    /**
     * This applies to only container Objects. This is invoked only if there is a corresponding listener.
     *
     */
    public void stream(EntryImpl currentEntry, StreamCodec codec) throws IOException {


    }

    /**
     * This should read the minimal data about the entry . if the data is a primitive type ,
     * read the whole thing
     */
    public void lazyRead(EntryImpl entry, StreamCodec codec) throws IOException {

    }

    /**
     * Read the entry as an Object. The behavior should be similar to that of {@link JavaBinCodec#readObject(DataInputInputStream)}
     */
    public Object readObject(StreamCodec codec, EntryImpl entry) throws IOException {
      throw new RuntimeException("Unsupported object : " + this.name());
    }

    /**
     * Read the entry from and discard the data. Do not create any objects
     */
    public void skip(EntryImpl entry, StreamCodec codec) throws IOException {
      if (entry.tag.type == DataEntry.Type.KEYVAL_ITER || entry.tag.type == DataEntry.Type.ENTRY_ITER) {
        entry.entryListener = null;
        stream(entry, codec);
      } else if (!entry.tag.type.isPrimitive) {
        readObject(codec, entry);
      }

    }
  }

  static final private Tag[] lower5BitTags = new Tag[32];

  static {
    for (Tag tag : Tag.values()) {
      if (tag.isLower5Bits) {
        lower5BitTags[tag.code] = tag;
      }
    }
  }

  public static void main(String[] args) {
    for (int i = 0; i < lower5BitTags.length; i++) {
      Tag tag = lower5BitTags[i];
      if (tag == null) continue;
      System.out.println(tag.name() + " : " + tag.code + (tag.isLower5Bits ? " lower" : " upper"));
    }
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void addObj(DataEntry e) {
    if (e.type().isContainer) {
      Object ctx = e.type() == DataEntry.Type.KEYVAL_ITER ?
          new LinkedHashMap(getSize(e)) :
          new ArrayList(getSize(e));
      if (e.ctx() != null) {
        if (e.isKeyValEntry()) {
          ((Map) e.ctx()).put(e.name(), ctx);
        } else {
          ((Collection) e.ctx()).add(ctx);
        }
      }
      e.listenContainer(ctx, getEntryListener());
    } else {
      Object val = e.val();
      if (val instanceof Utf8CharSequence) val = ((Utf8CharSequence) val).clone();
      if (e.ctx() != null) {
        if (e.isKeyValEntry()) {
          ((Map) e.ctx()).put(e.name(), val);
        } else {
          ((Collection) e.ctx()).add(val);
        }
      }
    }
  }

  private static int getSize(DataEntry e) {
    int sz = e.length();
    if (sz == -1) sz = e.type() == DataEntry.Type.KEYVAL_ITER ? 16 : 10;
    return sz;
  }


  public static EntryListener getEntryListener() {
    return ENTRY_LISTENER;
  }


  static final EntryListener ENTRY_LISTENER = FastJavaBinDecoder::addObj;


}
