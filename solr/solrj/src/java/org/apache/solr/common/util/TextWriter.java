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
import java.io.Writer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.PushWriter;

//Base interface for all text based writers
public interface TextWriter extends PushWriter {

  default void writeVal(String name, Object val) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)
    //    (see a patch on LUCENE-3041 for inspiration)

    // go in order of most common to least common, however some of the more general types like Map belong towards the end
    if (val == null) {
      writeNull(name);
    } else if (val instanceof CharSequence) {
      writeStr(name, val.toString(), true);
      // micro-optimization... using toString() avoids a cast first
    } else if (val instanceof Number) {
      writeNumber(name, (Number) val);
    } else if (val instanceof Boolean) {
      writeBool(name, (Boolean) val);
    } else if (val instanceof AtomicBoolean)  {
      writeBool(name, ((AtomicBoolean) val).get());
    } else if (val instanceof Date) {
      writeDate(name, (Date) val);
    } else if (val instanceof NamedList) {
      writeNamedList(name, (NamedList)val);
    } else if (val instanceof Path) {
      writeStr(name, ((Path) val).toAbsolutePath().toString(), true);
    } else if (val instanceof IteratorWriter) {
      writeIterator(name, (IteratorWriter) val);
    } else if (val instanceof MapWriter) {
      writeMap(name, (MapWriter) val);
    } else if (val instanceof MapSerializable) {
      //todo find a better way to reuse the map more efficiently
      writeMap(name, ((MapSerializable) val).toMap(new LinkedHashMap<>()), false, true);
    } else if (val instanceof Map) {
      writeMap(name, (Map)val, false, true);
    } else if (val instanceof Iterator) { // very generic; keep towards the end
      writeArray(name, (Iterator) val);
    } else if (val instanceof Iterable) { // very generic; keep towards the end
      writeArray(name,((Iterable)val).iterator());
    } else if (val instanceof Object[]) {
      writeArray(name,(Object[])val);
    } else if (val instanceof byte[]) {
      byte[] arr = (byte[])val;
      writeByteArr(name, arr, 0, arr.length);
    } else if (val instanceof EnumFieldValue) {
      writeStr(name, val.toString(), true);
    } else if (val instanceof WriteableValue) {
      ((WriteableValue)val).write(name, this);
    } else {
      // default... for debugging only.  Would be nice to "assert false" ?
      writeStr(name, val.getClass().getName() + ':' + val.toString(), true);
    }
  }

  void writeStr(String name, String val, boolean needsEscaping) throws IOException;

  void writeMap(String name, @SuppressWarnings({"rawtypes"})Map val, boolean excludeOuter, boolean isFirstVal) throws IOException;

  void writeArray(String name, @SuppressWarnings({"rawtypes"})Iterator val) throws IOException;

  void writeNull(String name) throws IOException;


  /** if this form of the method is called, val is the Java string form of an int */
  void writeInt(String name, String val) throws IOException;


  /** if this form of the method is called, val is the Java string form of a long */
  void writeLong(String name, String val) throws IOException;

  /** if this form of the method is called, val is the Java string form of a boolean */
  void writeBool(String name, String val) throws IOException;

  /** if this form of the method is called, val is the Java string form of a float */
  void writeFloat(String name, String val) throws IOException;


  /** if this form of the method is called, val is the Java string form of a double */
  void writeDouble(String name, String val) throws IOException;


  /** if this form of the method is called, val is the Solr ISO8601 based date format */
  void writeDate(String name, String val) throws IOException;

  void writeNamedList(String name, @SuppressWarnings({"rawtypes"})NamedList val) throws IOException;

  Writer getWriter();

  default void writeNumber(String name, Number val) throws IOException {
    if (val instanceof Integer) {
      writeInt(name, val.toString());
    } else if (val instanceof Long) {
      writeLong(name, val.toString());
    } else if (val instanceof Float) {
      // we pass the float instead of using toString() because
      // it may need special formatting. same for double.
      writeFloat(name, val.floatValue());
    } else if (val instanceof Double) {
      writeDouble(name, val.doubleValue());
    } else if (val instanceof Short) {
      writeInt(name, val.toString());
    } else if (val instanceof Byte) {
      writeInt(name, val.toString());
    } else if (val instanceof AtomicInteger) {
      writeInt(name, ((AtomicInteger) val).get());
    } else if (val instanceof AtomicLong) {
      writeLong(name, ((AtomicLong) val).get());
    } else {
      // default... for debugging only
      writeStr(name, val.getClass().getName() + ':' + val.toString(), true);
    }
  }

  default void writeArray(String name, Object[] val) throws IOException {
    writeArray(name, Arrays.asList(val));
  }

  default void writeArray(String name, @SuppressWarnings({"rawtypes"})List l) throws IOException {
    writeArray(name, l.iterator());
  }


  default void writeDate(String name, Date val) throws IOException {
    writeDate(name, val.toInstant().toString());
  }

  default void writeByteArr(String name, byte[] buf, int offset, int len) throws IOException {
    writeStr(name, Base64.byteArrayToBase64(buf, offset, len), false);
  }

  default void writeInt(String name, int val) throws IOException {
    writeInt(name,Integer.toString(val));
  }

  default void writeLong(String name, long val) throws IOException {
    writeLong(name,Long.toString(val));
  }


  default void writeBool(String name, boolean val) throws IOException {
    writeBool(name,Boolean.toString(val));
  }


  default void writeFloat(String name, float val) throws IOException {
    String s = Float.toString(val);
    // If it's not a normal number, write the value as a string instead.
    // The following test also handles NaN since comparisons are always false.
    if (val > Float.NEGATIVE_INFINITY && val < Float.POSITIVE_INFINITY) {
      writeFloat(name,s);
    } else {
      writeStr(name,s,false);
    }
  }


  default void writeDouble(String name, double val) throws IOException {
    String s = Double.toString(val);
    // If it's not a normal number, write the value as a string instead.
    // The following test also handles NaN since comparisons are always false.
    if (val > Double.NEGATIVE_INFINITY && val < Double.POSITIVE_INFINITY) {
      writeDouble(name,s);
    } else {
      writeStr(name,s,false);
    }
  }
  default void writeBool(String name , Boolean val) throws IOException {
    writeBool(name, val.toString());
  }

  @Override
  default void writeMap(MapWriter mw) throws IOException {
    //todo
  }

  default void writeMap(String name, MapWriter mw) throws IOException {
    writeMap(mw);
  }

  @Override
  default void writeIterator(IteratorWriter iw) throws IOException {
    /*todo*/
  }

  default void writeIterator(String name, IteratorWriter iw) throws IOException {
    writeIterator(iw);
  }

  default void indent() throws IOException {
    if (doIndent()) indent(level());
  }
  int incLevel();
  int decLevel();
  TextWriter setIndent(boolean doIndent);
  int level();
  boolean doIndent();
  default void indent(int lev) throws IOException {
    getWriter().write(SolrJSONWriter.indentChars, 0, Math.min((lev<<1)+1, SolrJSONWriter.indentChars.length));
  }

}
