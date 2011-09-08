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
package org.apache.solr.common.util;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.nio.ByteBuffer;

/**
 * The class is designed to optimaly serialize/deserialize any supported types in Solr response. As we know there are only a limited type of
 * items this class can do it with very minimal amount of payload and code. There are 15 known types and if there is an
 * object in the object tree which does not fall into these types, It must be converted to one of these. Implement an
 * ObjectResolver and pass it over It is expected that this class is used on both end of the pipes. The class has one
 * read method and one write method for each of the datatypes
 * <p/>
 * Note -- Never re-use an instance of this class for more than one marshal or unmarshall operation. Always create a new
 * instance.
 */
public class JavaBinCodec {

  public static final byte
          NULL = 0,
          BOOL_TRUE = 1,
          BOOL_FALSE = 2,
          BYTE = 3,
          SHORT = 4,
          DOUBLE = 5,
          INT = 6,
          LONG = 7,
          FLOAT = 8,
          DATE = 9,
          MAP = 10,
          SOLRDOC = 11,
          SOLRDOCLST = 12,
          BYTEARR = 13,
          ITERATOR = 14,
          /**
           * this is a special tag signals an end. No value is associated with it
           */
          END = 15,

          SOLRINPUTDOC = 16,

          // types that combine tag + length (or other info) in a single byte
          TAG_AND_LEN = (byte) (1 << 5),
          STR = (byte) (1 << 5),
          SINT = (byte) (2 << 5),
          SLONG = (byte) (3 << 5),
          ARR = (byte) (4 << 5), //
          ORDERED_MAP = (byte) (5 << 5), // SimpleOrderedMap (a NamedList subclass, and more common)
          NAMED_LST = (byte) (6 << 5), // NamedList
          EXTERN_STRING = (byte) (7 << 5);


  private static byte VERSION = 2;
  private ObjectResolver resolver;
  protected FastOutputStream daos;

  public JavaBinCodec() {
  }

  public JavaBinCodec(ObjectResolver resolver) {
    this.resolver = resolver;
  }

  public void marshal(Object nl, OutputStream os) throws IOException {
    init(FastOutputStream.wrap(os));
    try {
      daos.writeByte(VERSION);
      writeVal(nl);
    } finally {
      daos.flushBuffer();
    }
  }

  /** expert: sets a new output stream */
  public void init(FastOutputStream os) {
    daos = os;
  }

  byte version;

  public Object unmarshal(InputStream is) throws IOException {
    FastInputStream dis = FastInputStream.wrap(is);
    version = dis.readByte();
    if (version != VERSION) {
      throw new RuntimeException("Invalid version (expected " + VERSION +
          ", but " + version + ") or the data in not in 'javabin' format");
    }
    return readVal(dis);
  }


  public SimpleOrderedMap<Object> readOrderedMap(FastInputStream dis) throws IOException {
    int sz = readSize(dis);
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<Object>();
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(dis);
      Object val = readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public NamedList<Object> readNamedList(FastInputStream dis) throws IOException {
    int sz = readSize(dis);
    NamedList<Object> nl = new NamedList<Object>();
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(dis);
      Object val = readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public void writeNamedList(NamedList<?> nl) throws IOException {
    writeTag(nl instanceof SimpleOrderedMap ? ORDERED_MAP : NAMED_LST, nl.size());
    for (int i = 0; i < nl.size(); i++) {
      String name = nl.getName(i);
      writeExternString(name);
      Object val = nl.getVal(i);
      writeVal(val);
    }
  }

  public void writeVal(Object val) throws IOException {
    if (writeKnownType(val)) {
      return;
    } else {
      Object tmpVal = val;
      if (resolver != null) {
        tmpVal = resolver.resolve(val, this);
        if (tmpVal == null) return; // null means the resolver took care of it fully
        if (writeKnownType(tmpVal)) return;
      }
    }

    writeVal(val.getClass().getName() + ':' + val.toString());
  }

  protected static final Object END_OBJ = new Object();

  protected byte tagByte;

  public Object readVal(FastInputStream dis) throws IOException {
    tagByte = dis.readByte();

    // if ((tagByte & 0xe0) == 0) {
    // if top 3 bits are clear, this is a normal tag

    // OK, try type + size in single byte
    switch (tagByte >>> 5) {
      case STR >>> 5:
        return readStr(dis);
      case SINT >>> 5:
        return readSmallInt(dis);
      case SLONG >>> 5:
        return readSmallLong(dis);
      case ARR >>> 5:
        return readArray(dis);
      case ORDERED_MAP >>> 5:
        return readOrderedMap(dis);
      case NAMED_LST >>> 5:
        return readNamedList(dis);
      case EXTERN_STRING >>> 5:
        return readExternString(dis);
    }

    switch (tagByte) {
      case NULL:
        return null;
      case DATE:
        return new Date(dis.readLong());
      case INT:
        return dis.readInt();
      case BOOL_TRUE:
        return Boolean.TRUE;
      case BOOL_FALSE:
        return Boolean.FALSE;
      case FLOAT:
        return dis.readFloat();
      case DOUBLE:
        return dis.readDouble();
      case LONG:
        return dis.readLong();
      case BYTE:
        return dis.readByte();
      case SHORT:
        return dis.readShort();
      case MAP:
        return readMap(dis);
      case SOLRDOC:
        return readSolrDocument(dis);
      case SOLRDOCLST:
        return readSolrDocumentList(dis);
      case BYTEARR:
        return readByteArray(dis);
      case ITERATOR:
        return readIterator(dis);
      case END:
        return END_OBJ;
      case SOLRINPUTDOC:
        return readSolrInputDocument(dis);
    }

    throw new RuntimeException("Unknown type " + tagByte);
  }

  public boolean writeKnownType(Object val) throws IOException {
    if (writePrimitive(val)) return true;
    if (val instanceof NamedList) {
      writeNamedList((NamedList<?>) val);
      return true;
    }
    if (val instanceof SolrDocumentList) { // SolrDocumentList is a List, so must come before List check
      writeSolrDocumentList((SolrDocumentList) val);
      return true;
    }
    if (val instanceof Collection) {
      writeArray((Collection) val);
      return true;
    }
    if (val instanceof Object[]) {
      writeArray((Object[]) val);
      return true;
    }
    if (val instanceof SolrDocument) {
      //this needs special treatment to know which fields are to be written
      if (resolver == null) {
        writeSolrDocument((SolrDocument) val);
      } else {
        Object retVal = resolver.resolve(val, this);
        if (retVal != null) {
          if (retVal instanceof SolrDocument) {
            writeSolrDocument((SolrDocument) retVal);
          } else {
            writeVal(retVal);
          }
        }
      }
      return true;
    }
    if (val instanceof SolrInputDocument) {
      writeSolrInputDocument((SolrInputDocument)val);
      return true;
    }
    if (val instanceof Map) {
      writeMap((Map) val);
      return true;
    }
    if (val instanceof Iterator) {
      writeIterator((Iterator) val);
      return true;
    }
    if (val instanceof Iterable) {
      writeIterator(((Iterable) val).iterator());
      return true;
    }
    return false;
  }

  public void writeTag(byte tag) throws IOException {
    daos.writeByte(tag);
  }

  public void writeTag(byte tag, int size) throws IOException {
    if ((tag & 0xe0) != 0) {
      if (size < 0x1f) {
        daos.writeByte(tag | size);
      } else {
        daos.writeByte(tag | 0x1f);
        writeVInt(size - 0x1f, daos);
      }
    } else {
      daos.writeByte(tag);
      writeVInt(size, daos);
    }
  }

  public void writeByteArray(byte[] arr, int offset, int len) throws IOException {
    writeTag(BYTEARR, len);
    daos.write(arr, offset, len);
  }

  public byte[] readByteArray(FastInputStream dis) throws IOException {
    byte[] arr = new byte[readVInt(dis)];
    dis.readFully(arr);
    return arr;
  }

  public void writeSolrDocument(SolrDocument doc) throws IOException {
    writeTag(SOLRDOC);
    writeTag(ORDERED_MAP, doc.size());
    for (Map.Entry<String, Object> entry : doc) {
      String name = entry.getKey();
      writeExternString(name);
      Object val = entry.getValue();
      writeVal(val);
    }
  }

  public SolrDocument readSolrDocument(FastInputStream dis) throws IOException {
    NamedList nl = (NamedList) readVal(dis);
    SolrDocument doc = new SolrDocument();
    for (int i = 0; i < nl.size(); i++) {
      String name = nl.getName(i);
      Object val = nl.getVal(i);
      doc.setField(name, val);
    }
    return doc;
  }

  public SolrDocumentList readSolrDocumentList(FastInputStream dis) throws IOException {
    SolrDocumentList solrDocs = new SolrDocumentList();
    List list = (List) readVal(dis);
    solrDocs.setNumFound((Long) list.get(0));
    solrDocs.setStart((Long) list.get(1));
    solrDocs.setMaxScore((Float) list.get(2));

    @SuppressWarnings("unchecked")
    List<SolrDocument> l = (List<SolrDocument>) readVal(dis);
    solrDocs.addAll(l);
    return solrDocs;
  }

  public void writeSolrDocumentList(SolrDocumentList docs)
          throws IOException {
    writeTag(SOLRDOCLST);
    List<Number> l = new ArrayList<Number>(3);
    l.add(docs.getNumFound());
    l.add(docs.getStart());
    l.add(docs.getMaxScore());
    writeArray(l);
    writeArray(docs);
  }

  public SolrInputDocument readSolrInputDocument(FastInputStream dis) throws IOException {
    int sz = readVInt(dis);
    float docBoost = (Float)readVal(dis);
    SolrInputDocument sdoc = new SolrInputDocument();
    sdoc.setDocumentBoost(docBoost);
    for (int i = 0; i < sz; i++) {
      float boost = 1.0f;
      String fieldName;
      Object boostOrFieldName = readVal(dis);
      if (boostOrFieldName instanceof Float) {
        boost = (Float)boostOrFieldName;
        fieldName = (String)readVal(dis);
      } else {
        fieldName = (String)boostOrFieldName;
      }
      Object fieldVal = readVal(dis);
      sdoc.setField(fieldName, fieldVal, boost);
    }
    return sdoc;
  }

  public void writeSolrInputDocument(SolrInputDocument sdoc) throws IOException {
    writeTag(SOLRINPUTDOC, sdoc.size());
    writeFloat(sdoc.getDocumentBoost());
    for (SolrInputField inputField : sdoc.values()) {
      if (inputField.getBoost() != 1.0f) {
        writeFloat(inputField.getBoost());
      }
      writeExternString(inputField.getName());
      writeVal(inputField.getValue());
    }
  }


  public Map<Object,Object> readMap(FastInputStream dis)
          throws IOException {
    int sz = readVInt(dis);
    Map<Object,Object> m = new LinkedHashMap<Object,Object>();
    for (int i = 0; i < sz; i++) {
      Object key = readVal(dis);
      Object val = readVal(dis);
      m.put(key, val);

    }
    return m;
  }

  public void writeIterator(Iterator iter) throws IOException {
    writeTag(ITERATOR);
    while (iter.hasNext()) {
      writeVal(iter.next());
    }
    writeVal(END_OBJ);
  }

  public List<Object> readIterator(FastInputStream fis) throws IOException {
    ArrayList<Object> l = new ArrayList<Object>();
    while (true) {
      Object o = readVal(fis);
      if (o == END_OBJ) break;
      l.add(o);
    }
    return l;
  }

  public void writeArray(List l) throws IOException {
    writeTag(ARR, l.size());
    for (int i = 0; i < l.size(); i++) {
      writeVal(l.get(i));
    }
  }

  public void writeArray(Collection coll) throws IOException {
    writeTag(ARR, coll.size());
    for (Object o : coll) {
      writeVal(o);
    }

  }

  public void writeArray(Object[] arr) throws IOException {
    writeTag(ARR, arr.length);
    for (int i = 0; i < arr.length; i++) {
      Object o = arr[i];
      writeVal(o);
    }
  }

  public List<Object> readArray(FastInputStream dis) throws IOException {
    int sz = readSize(dis);
    ArrayList<Object> l = new ArrayList<Object>(sz);
    for (int i = 0; i < sz; i++) {
      l.add(readVal(dis));
    }
    return l;
  }

  /**
   * write the string as tag+length, with length being the number of UTF-8 bytes
   */
  public void writeStr(String s) throws IOException {
    if (s == null) {
      writeTag(NULL);
      return;
    }
    int end = s.length();
    int maxSize = end * 4;
    if (bytes == null || bytes.length < maxSize) bytes = new byte[maxSize];
    int upto = 0;
    for(int i=0;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        bytes[upto++] = (byte) code;
      else if (code < 0x800) {
        bytes[upto++] = (byte) (0xC0 | (code >> 6));
        bytes[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        bytes[upto++] = (byte)(0xE0 | (code >> 12));
        bytes[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        bytes[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            bytes[upto++] = (byte)(0xF0 | (utf32 >> 18));
            bytes[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            bytes[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            bytes[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        bytes[upto++] = (byte) 0xEF;
        bytes[upto++] = (byte) 0xBF;
        bytes[upto++] = (byte) 0xBD;
      }
    }
    writeTag(STR, upto);
    daos.write(bytes, 0, upto);
  }

  byte[] bytes;
  char[] chars;

  public String readStr(FastInputStream dis) throws IOException {
    int sz = readSize(dis);
    if (chars == null || chars.length < sz) chars = new char[sz];
    if (bytes == null || bytes.length < sz) bytes = new byte[sz];
    dis.readFully(bytes, 0, sz);
    int outUpto=0;
    for (int i = 0; i < sz;) {
      final int b = bytes[i++]&0xff;
      final int ch;
      if (b < 0xc0) {
        assert b < 0x80;
        ch = b;
      } else if (b < 0xe0) {
        ch = ((b&0x1f)<<6) + (bytes[i++]&0x3f);
      } else if (b < 0xf0) {
        ch = ((b&0xf)<<12) + ((bytes[i++]&0x3f)<<6) + (bytes[i++]&0x3f);
      } else {
        assert b < 0xf8;
        ch = ((b&0x7)<<18) + ((bytes[i++]&0x3f)<<12) + ((bytes[i++]&0x3f)<<6) + (bytes[i++]&0x3f);
      }
      if (ch <= 0xFFFF) {
        // target is a character <= 0xFFFF
        chars[outUpto++] = (char) ch;
      } else {
        // target is a character in range 0xFFFF - 0x10FFFF
        final int chHalf = ch - 0x10000;
        chars[outUpto++] = (char) ((chHalf >> 0xA) + 0xD800);
        chars[outUpto++] = (char) ((chHalf & 0x3FF) + 0xDC00);
      }
    }
    return new String(chars, 0, outUpto);
  }

  public void writeInt(int val) throws IOException {
    if (val > 0) {
      int b = SINT | (val & 0x0f);

      if (val >= 0x0f) {
        b |= 0x10;
        daos.writeByte(b);
        writeVInt(val >>> 4, daos);
      } else {
        daos.writeByte(b);
      }

    } else {
      daos.writeByte(INT);
      daos.writeInt(val);
    }
  }

  public int readSmallInt(FastInputStream dis) throws IOException {
    int v = tagByte & 0x0F;
    if ((tagByte & 0x10) != 0)
      v = (readVInt(dis) << 4) | v;
    return v;
  }


  public void writeLong(long val) throws IOException {
    if ((val & 0xff00000000000000L) == 0) {
      int b = SLONG | ((int) val & 0x0f);
      if (val >= 0x0f) {
        b |= 0x10;
        daos.writeByte(b);
        writeVLong(val >>> 4, daos);
      } else {
        daos.writeByte(b);
      }
    } else {
      daos.writeByte(LONG);
      daos.writeLong(val);
    }
  }

  public long readSmallLong(FastInputStream dis) throws IOException {
    long v = tagByte & 0x0F;
    if ((tagByte & 0x10) != 0)
      v = (readVLong(dis) << 4) | v;
    return v;
  }

  public void writeFloat(float val) throws IOException {
    daos.writeByte(FLOAT);
    daos.writeFloat(val);
  }

  public boolean writePrimitive(Object val) throws IOException {
    if (val == null) {
      daos.writeByte(NULL);
      return true;
    } else if (val instanceof String) {
      writeStr((String) val);
      return true;
    } else if (val instanceof Integer) {
      writeInt(((Integer) val).intValue());
      return true;
    } else if (val instanceof Long) {
      writeLong(((Long) val).longValue());
      return true;
    } else if (val instanceof Float) {
      writeFloat(((Float) val).floatValue());
      return true;
    } else if (val instanceof Date) {
      daos.writeByte(DATE);
      daos.writeLong(((Date) val).getTime());
      return true;
    } else if (val instanceof Boolean) {
      if ((Boolean) val) daos.writeByte(BOOL_TRUE);
      else daos.writeByte(BOOL_FALSE);
      return true;
    } else if (val instanceof Double) {
      daos.writeByte(DOUBLE);
      daos.writeDouble(((Double) val).doubleValue());
      return true;
    } else if (val instanceof Byte) {
      daos.writeByte(BYTE);
      daos.writeByte(((Byte) val).intValue());
      return true;
    } else if (val instanceof Short) {
      daos.writeByte(SHORT);
      daos.writeShort(((Short) val).intValue());
      return true;
    } else if (val instanceof byte[]) {
      writeByteArray((byte[]) val, 0, ((byte[]) val).length);
      return true;
    } else if (val instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) val;
      writeByteArray(buf.array(),buf.position(),buf.limit() - buf.position());
      return true;
    } else if (val == END_OBJ) {
      writeTag(END);
      return true;
    }
    return false;
  }


  public void writeMap(Map<?,?> val) throws IOException {
    writeTag(MAP, val.size());
    for (Map.Entry<?,?> entry : val.entrySet()) {
      Object key = entry.getKey();
      if (key instanceof String) {
        writeExternString((String) key);
      } else {
        writeVal(key);
      }
      writeVal(entry.getValue());
    }
  }


  public int readSize(FastInputStream in) throws IOException {
    int sz = tagByte & 0x1f;
    if (sz == 0x1f) sz += readVInt(in);
    return sz;
  }


  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the length of a
   * collection/array/map In most of the cases the length can be represented in one byte (length < 127) so it saves 3
   * bytes/object
   *
   * @param i
   * @param out
   *
   * @throws IOException
   */
  public static void writeVInt(int i, FastOutputStream out) throws IOException {
    while ((i & ~0x7F) != 0) {
      out.writeByte((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    out.writeByte((byte) i);
  }

  /**
   * The counterpart for the above
   *
   * @param in
   *
   * @return the int value
   *
   * @throws IOException
   */
  public static int readVInt(FastInputStream in) throws IOException {
    byte b = in.readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
  }


  public static void writeVLong(long i, FastOutputStream out) throws IOException {
    while ((i & ~0x7F) != 0) {
      out.writeByte((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    out.writeByte((byte) i);
  }

  public static long readVLong(FastInputStream in) throws IOException {
    byte b = in.readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      i |= (long) (b & 0x7F) << shift;
    }
    return i;
  }

  private int stringsCount = 0;
  private Map<String, Integer> stringsMap;
  private List<String> stringsList;

  public void writeExternString(String s) throws IOException {
    if (s == null) {
      writeTag(NULL);
      return;
    }
    Integer idx = stringsMap == null ? null : stringsMap.get(s);
    if (idx == null) idx = 0;
    writeTag(EXTERN_STRING, idx);
    if (idx == 0) {
      writeStr(s);
      if (stringsMap == null) stringsMap = new HashMap<String, Integer>();
      stringsMap.put(s, ++stringsCount);
    }

  }

  public String readExternString(FastInputStream fis) throws IOException {
    int idx = readSize(fis);
    if (idx != 0) {// idx != 0 is the index of the extern string
      return stringsList.get(idx - 1);
    } else {// idx == 0 means it has a string value
      String s = (String) readVal(fis);
      if (stringsList == null) stringsList = new ArrayList<String>();
      stringsList.add(s);
      return s;
    }
  }


  public static interface ObjectResolver {
    public Object resolve(Object o, JavaBinCodec codec) throws IOException;
  }


}
