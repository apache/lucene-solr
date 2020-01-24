/*
 *  Copyright 2006- Yonik Seeley
 *
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

package org.noggit;

import java.util.*;


public class JSONWriter {

  /**
   * Implement this interface on your class to support serialization
   */
  public static interface Writable {
    public void write(JSONWriter writer);
  }

  protected int level;
  protected int indent;
  protected final CharArr out;

  /**
   * @param out        the CharArr to write the output to.
   * @param indentSize The number of space characters to use as an indent (default 2). 0=newlines but no spaces, -1=no indent at all.
   */
  public JSONWriter(CharArr out, int indentSize) {
    this.out = out;
    this.indent = indentSize;
  }

  public JSONWriter(CharArr out) {
    this(out, 2);
  }

  public void setIndentSize(int indentSize) {
    this.indent = indentSize;
  }

  public void indent() {
    if (indent >= 0) {
      out.write('\n');
      if (indent > 0) {
        int spaces = level * indent;
        out.reserve(spaces);
        for (int i = 0; i < spaces; i++) {
          out.unsafeWrite(' ');
        }
      }
    }
  }

  public void write(Object o) {
    // NOTE: an instance-of chain was about 50% faster than hashing on the classes, even with perfect hashing.
    if (o == null) {
      writeNull();
    } else if (o instanceof String) {
      writeString((String) o);
    } else if (o instanceof Number) {
      if (o instanceof Integer || o instanceof Long) {
        write(((Number) o).longValue());
      } else if (o instanceof Float || o instanceof Double) {
        write(((Number) o).doubleValue());
      } else {
        CharArr arr = new CharArr();
        arr.write(o.toString());
        writeNumber(arr);
      }
    } else if (o instanceof Map) {
      write((Map<?, ?>) o);
    } else if (o instanceof Collection) {
      write((Collection<?>) o);
    } else if (o instanceof Boolean) {
      write(((Boolean) o).booleanValue());
    } else if (o instanceof CharSequence) {
      writeString((CharSequence) o);
    } else if (o instanceof Writable) {
      ((Writable) o).write(this);
    } else if (o instanceof Object[]) {
      write(Arrays.asList((Object[]) o));
    } else if (o instanceof int[]) {
      write((int[]) o);
    } else if (o instanceof float[]) {
      write((float[]) o);
    } else if (o instanceof long[]) {
      write((long[]) o);
    } else if (o instanceof double[]) {
      write((double[]) o);
    } else if (o instanceof short[]) {
      write((short[]) o);
    } else if (o instanceof boolean[]) {
      write((boolean[]) o);
    } else if (o instanceof char[]) {
      write((char[]) o);
    } else if (o instanceof byte[]) {
      write((byte[]) o);
    } else {
      handleUnknownClass(o);
    }
  }

  /**
   * Override this method for custom handling of unknown classes.  Also see the Writable interface.
   */
  public void handleUnknownClass(Object o) {
    writeString(o.toString());
  }

  public void write(Map<?, ?> val) {
    startObject();
    int sz = val.size();
    boolean first = true;
    for (Map.Entry<?, ?> entry : val.entrySet()) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      if (sz > 1) indent();
      writeString(entry.getKey().toString());
      writeNameSeparator();
      write(entry.getValue());
    }
    endObject();
  }

  public void write(Collection<?> val) {
    startArray();
    int sz = val.size();
    boolean first = true;
    for (Object o : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      if (sz > 1) indent();
      write(o);
    }
    endArray();
  }

  /**
   * A byte[] may be either a single logical value, or a list of small integers.
   * It's up to the implementation to decide.
   */
  public void write(byte[] val) {
    startArray();
    boolean first = true;
    for (short v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }

  public void write(short[] val) {
    startArray();
    boolean first = true;
    for (short v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }

  public void write(int[] val) {
    startArray();
    boolean first = true;
    for (int v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }

  public void write(long[] val) {
    startArray();
    boolean first = true;
    for (long v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }

  public void write(float[] val) {
    startArray();
    boolean first = true;
    for (float v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }

  public void write(double[] val) {
    startArray();
    boolean first = true;
    for (double v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }

  public void write(boolean[] val) {
    startArray();
    boolean first = true;
    for (boolean v : val) {
      if (first) {
        first = false;
      } else {
        writeValueSeparator();
      }
      write(v);
    }
    endArray();
  }


  public void write(short number) {
    write((int) number);
  }

  public void write(byte number) {
    write((int) number);
  }


  public void writeNull() {
    JSONUtil.writeNull(out);
  }

  public void writeString(String str) {
    JSONUtil.writeString(str, 0, str.length(), out);
  }

  public void writeString(CharSequence str) {
    JSONUtil.writeString(str, 0, str.length(), out);
  }

  public void writeString(CharArr str) {
    JSONUtil.writeString(str, out);
  }

  public void writeStringStart() {
    out.write('"');
  }

  public void writeStringChars(CharArr partialStr) {
    JSONUtil.writeStringPart(partialStr.getArray(), partialStr.getStart(), partialStr.getEnd(), out);
  }

  public void writeStringEnd() {
    out.write('"');
  }

  public void write(long number) {
    JSONUtil.writeNumber(number, out);
  }

  public void write(int number) {
    JSONUtil.writeNumber(number, out);
  }

  public void write(double number) {
    JSONUtil.writeNumber(number, out);
  }

  public void write(float number) {
    JSONUtil.writeNumber(number, out);
  }

  public void write(boolean bool) {
    JSONUtil.writeBoolean(bool, out);
  }

  public void write(char[] val) {
    JSONUtil.writeString(val, 0, val.length, out);
  }

  public void writeNumber(CharArr digits) {
    out.write(digits);
  }

  public void writePartialNumber(CharArr digits) {
    out.write(digits);
  }

  public void startObject() {
    out.write('{');
    level++;
  }

  public void endObject() {
    out.write('}');
    level--;
  }

  public void startArray() {
    out.write('[');
    level++;
  }

  public void endArray() {
    out.write(']');
    level--;
  }

  public void writeValueSeparator() {
    out.write(',');
  }

  public void writeNameSeparator() {
    out.write(':');
  }

}
