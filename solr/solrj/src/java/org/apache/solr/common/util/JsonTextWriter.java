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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;

public interface JsonTextWriter extends TextWriter {
  char[] hexdigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  String JSON_NL_MAP = "map";
  String JSON_NL_FLAT = "flat";
  String JSON_NL_ARROFARR = "arrarr";
  String JSON_NL_ARROFMAP = "arrmap";
  String JSON_NL_ARROFNTV = "arrntv";
  String JSON_NL_STYLE = "json.nl";

  String getNamedListStyle();

  void _writeChar(char c) throws IOException;

  void _writeStr(String s) throws IOException;

  default void writeMapOpener(int size) throws IOException, IllegalArgumentException {
    _writeChar('{');
  }

  default void writeMapSeparator() throws IOException {
    _writeChar(',');
  }

  default void writeMapCloser() throws IOException {
    _writeChar('}');
  }

  default void writeArrayOpener(int size) throws IOException, IllegalArgumentException {
    _writeChar('[');
  }

  default void writeArraySeparator() throws IOException {
    _writeChar(',');
  }

  default void writeArrayCloser() throws IOException {
    _writeChar(']');
  }

  default void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    // it might be more efficient to use a stringbuilder or write substrings
    // if writing chars to the stream is slow.
    if (needsEscaping) {


     /* http://www.ietf.org/internet-drafts/draft-crockford-jsonorg-json-04.txt
      All Unicode characters may be placed within
      the quotation marks except for the characters which must be
      escaped: quotation mark, reverse solidus, and the control
      characters (U+0000 through U+001F).
     */
      _writeChar('"');

      for (int i = 0; i < val.length(); i++) {
        char ch = val.charAt(i);
        if ((ch > '#' && ch != '\\' && ch < '\u2028') || ch == ' ') { // fast path
          _writeChar(ch);
          continue;
        }
        switch (ch) {
          case '"':
          case '\\':
            _writeChar('\\');
            _writeChar(ch);
            break;
          case '\r':
            _writeChar('\\');
            _writeChar('r');
            break;
          case '\n':
            _writeChar('\\');
            _writeChar('n');
            break;
          case '\t':
            _writeChar('\\');
            _writeChar('t');
            break;
          case '\b':
            _writeChar('\\');
            _writeChar('b');
            break;
          case '\f':
            _writeChar('\\');
            _writeChar('f');
            break;
          case '\u2028': // fallthrough
          case '\u2029':
            unicodeEscape(getWriter(), ch);
            break;
          // case '/':
          default: {
            if (ch <= 0x1F) {
              unicodeEscape(getWriter(), ch);
            } else {
              _writeChar(ch);
            }
          }
        }
      }

      _writeChar('"');
    } else {
      _writeChar('"');
      _writeStr(val);
      _writeChar('"');
    }
  }

  default void writeIterator(IteratorWriter val) throws IOException {
    writeArrayOpener(-1);
    incLevel();
    val.writeIter(new IteratorWriter.ItemWriter() {
      boolean first = true;

      @Override
      public IteratorWriter.ItemWriter add(Object o) throws IOException {
        if (!first) {
          JsonTextWriter.this.indent();
          JsonTextWriter.this.writeArraySeparator();
        }
        JsonTextWriter.this.writeVal(null, o);
        first = false;
        return this;
      }
    });
    decLevel();
    writeArrayCloser();
  }

  default void writeMap(MapWriter val)
      throws IOException {
    writeMapOpener(-1);
    incLevel();

    val.writeMap(new MapWriter.EntryWriter() {
      boolean isFirst = true;

      @Override
      public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
        if (isFirst) {
          isFirst = false;
        } else {
          JsonTextWriter.this.writeMapSeparator();
        }
        JsonTextWriter.this.indent();
        JsonTextWriter.this.writeKey(k.toString(), true);
        writeVal(k.toString(), v);
        return this;
      }
    });
    decLevel();
    writeMapCloser();
  }

  default void writeKey(String fname, boolean needsEscaping) throws IOException {
    writeStr(null, fname, needsEscaping);
    _writeChar(':');
  }

  default void writeJsonIter(Iterator val) throws IOException {
    incLevel();
    boolean first = true;
    while (val.hasNext()) {
      if (!first) indent();
      writeVal(null, val.next());
      if (val.hasNext()) {
        writeArraySeparator();
      }
      first = false;
    }
    decLevel();
  }

  //
  // Primitive types
  //
  default void writeNull(String name) throws IOException {
    _writeStr("null");
  }


  default void writeInt(String name, String val) throws IOException {
    _writeStr(val);
  }

  default void writeLong(String name, String val) throws IOException {
    _writeStr(val);
  }

  default void writeBool(String name, String val) throws IOException {
    _writeStr(val);
  }

  default void writeFloat(String name, String val) throws IOException {
    _writeStr(val);
  }

  default void writeDouble(String name, String val) throws IOException {
    _writeStr(val);
  }

  default void writeDate(String name, String val) throws IOException {
    writeStr(name, val, false);
  }

  @SuppressWarnings("unchecked")
  default void writeMap(String name, Map val, boolean excludeOuter, boolean isFirstVal) throws IOException {
    if (!excludeOuter) {
      writeMapOpener(val.size());
      incLevel();
      isFirstVal = true;
    }

    boolean doIndent = excludeOuter || val.size() > 1;

    for (Map.Entry entry : (Set<Map.Entry>) val.entrySet()) {
      Object e = entry.getKey();
      String k = e == null ? "" : e.toString();
      Object v = entry.getValue();

      if (isFirstVal) {
        isFirstVal = false;
      } else {
        writeMapSeparator();
      }

      if (doIndent) indent();
      writeKey(k, true);
      writeVal(k, v);
    }

    if (!excludeOuter) {
      decLevel();
      writeMapCloser();
    }
  }


  default void writeArray(String name, List l) throws IOException {
    writeArrayOpener(l.size());
    writeJsonIter(l.iterator());
    writeArrayCloser();
  }

  default void writeArray(String name, Iterator val) throws IOException {
    writeArrayOpener(-1); // no trivial way to determine array size
    writeJsonIter(val);
    writeArrayCloser();
  }

  default void unicodeEscape(Appendable out, int ch) throws IOException {
    out.append('\\');
    out.append('u');
    out.append(hexdigits[(ch >>> 12)]);
    out.append(hexdigits[(ch >>> 8) & 0xf]);
    out.append(hexdigits[(ch >>> 4) & 0xf]);
    out.append(hexdigits[(ch) & 0xf]);
  }

  default void writeNamedList(String name, NamedList val) throws IOException {
    String namedListStyle = getNamedListStyle();
    if (val instanceof SimpleOrderedMap) {
      writeNamedListAsMapWithDups(name, val);
    } else if (namedListStyle == JSON_NL_FLAT) {
      writeNamedListAsFlat(name, val);
    } else if (namedListStyle == JSON_NL_MAP) {
      writeNamedListAsMapWithDups(name, val);
    } else if (namedListStyle == JSON_NL_ARROFARR) {
      writeNamedListAsArrArr(name, val);
    } else if (namedListStyle == JSON_NL_ARROFMAP) {
      writeNamedListAsArrMap(name, val);
    } else if (namedListStyle == JSON_NL_ARROFNTV) {
      throw new UnsupportedOperationException(namedListStyle
          + " namedListStyle must only be used with ArrayOfNameTypeValueJSONWriter");
    }
  }

  /**
   * Represents a NamedList directly as a JSON Object (essentially a Map)
   * Map null to "" and name mangle any repeated keys to avoid repeats in the
   * output.
   */
  default void writeNamedListAsMapMangled(String name, NamedList val) throws IOException {
    int sz = val.size();
    writeMapOpener(sz);
    incLevel();

    // In JSON objects (maps) we can't have null keys or duplicates...
    // map null to "" and append a qualifier to duplicates.
    //
    // a=123,a=456 will be mapped to {a=1,a__1=456}
    // Disad: this is ambiguous since a real key could be called a__1
    //
    // Another possible mapping could aggregate multiple keys to an array:
    // a=123,a=456 maps to a=[123,456]
    // Disad: this is ambiguous with a real single value that happens to be an array
    //
    // Both of these mappings have ambiguities.
    HashMap<String, Integer> repeats = new HashMap<>(4);

    boolean first = true;
    for (int i = 0; i < sz; i++) {
      String key = val.getName(i);
      if (key == null) key = "";

      if (first) {
        first = false;
        repeats.put(key, 0);
      } else {
        writeMapSeparator();

        Integer repeatCount = repeats.get(key);
        if (repeatCount == null) {
          repeats.put(key, 0);
        } else {
          String newKey = key;
          int newCount = repeatCount;
          do {  // avoid generated key clashing with a real key
            newKey = key + ' ' + (++newCount);
            repeatCount = repeats.get(newKey);
          } while (repeatCount != null);

          repeats.put(key, newCount);
          key = newKey;
        }
      }

      indent();
      writeKey(key, true);
      writeVal(key, val.getVal(i));
    }

    decLevel();
    writeMapCloser();
  }

  /**
   * Represents a NamedList directly as a JSON Object (essentially a Map)
   * repeating any keys if they are repeated in the NamedList.
   * null key is mapped to "".
   */
  // NamedList("a"=1,"bar"="foo",null=3,null=null) => {"a":1,"bar":"foo","":3,"":null}
  default void writeNamedListAsMapWithDups(String name, NamedList val) throws IOException {
    int sz = val.size();
    writeMapOpener(sz);
    incLevel();

    for (int i = 0; i < sz; i++) {
      if (i != 0) {
        writeMapSeparator();
      }

      String key = val.getName(i);
      if (key == null) key = "";
      indent();
      writeKey(key, true);
      writeVal(key, val.getVal(i));
    }

    decLevel();
    writeMapCloser();
  }

  // Represents a NamedList directly as an array of JSON objects...
  // NamedList("a"=1,"b"=2,null=3,null=null) => [{"a":1},{"b":2},3,null]
  default void writeNamedListAsArrMap(String name, NamedList val) throws IOException {
    int sz = val.size();
    indent();
    writeArrayOpener(sz);
    incLevel();

    boolean first = true;
    for (int i = 0; i < sz; i++) {
      String key = val.getName(i);

      if (first) {
        first = false;
      } else {
        writeArraySeparator();
      }

      indent();

      if (key == null) {
        writeVal(null, val.getVal(i));
      } else {
        writeMapOpener(1);
        writeKey(key, true);
        writeVal(key, val.getVal(i));
        writeMapCloser();
      }

    }

    decLevel();
    writeArrayCloser();
  }

  // Represents a NamedList directly as an array of JSON objects...
  // NamedList("a"=1,"b"=2,null=3,null=null) => [["a",1],["b",2],[null,3],[null,null]]
  default void writeNamedListAsArrArr(String name, NamedList val) throws IOException {
    int sz = val.size();
    indent();
    writeArrayOpener(sz);
    incLevel();

    boolean first = true;
    for (int i = 0; i < sz; i++) {
      String key = val.getName(i);

      if (first) {
        first = false;
      } else {
        writeArraySeparator();
      }

      indent();

      /*** if key is null, just write value???
       if (key==null) {
       writeVal(null,val.getVal(i));
       } else {
       ***/

      writeArrayOpener(1);
      incLevel();
      if (key == null) {
        writeNull(null);
      } else {
        writeStr(null, key, true);
      }
      writeArraySeparator();
      writeVal(key, val.getVal(i));
      decLevel();
      writeArrayCloser();
    }

    decLevel();
    writeArrayCloser();
  }

  // Represents a NamedList directly as an array with keys/values
  // interleaved.
  // NamedList("a"=1,"b"=2,null=3,null=null) => ["a",1,"b",2,null,3,null,null]
  default void writeNamedListAsFlat(String name, NamedList val) throws IOException {
    int sz = val.size();
    writeArrayOpener(sz * 2);
    incLevel();

    for (int i = 0; i < sz; i++) {
      if (i != 0) {
        writeArraySeparator();
      }
      String key = val.getName(i);
      indent();
      if (key == null) {
        writeNull(null);
      } else {
        writeStr(null, key, true);
      }
      writeArraySeparator();
      writeVal(key, val.getVal(i));
    }

    decLevel();
    writeArrayCloser();
  }


}
