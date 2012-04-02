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

package org.apache.noggit;

import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.Arrays;

public class TextSerializer {
  public void serialize(TextWriter writer, Map val) {
    writer.startObject();
    boolean first = true;
    for (Map.Entry entry : (Set<Map.Entry>)val.entrySet()) {
      if (first) {
        first = false;
      } else {
        writer.writeValueSeparator();
      }
      writer.writeString(entry.getKey().toString());
      writer.writeNameSeparator();
      serialize(writer, entry.getValue());
    }
    writer.endObject();
  }

  public void serialize(TextWriter writer, Collection val) {
    writer.startArray();
    boolean first = true;
    for (Object o : val) {
      if (first) {
        first = false;
      } else {
        writer.writeValueSeparator();
      }
      serialize(writer, o);
    }
    writer.endArray();
  }

  public void serialize(TextWriter writer, Object o) {
    if (o == null) {
      writer.writeNull();
    } else if (o instanceof CharSequence) {
      writer.writeString((CharSequence)o);
    } else if (o instanceof Number) {
      if (o instanceof Integer || o instanceof Long) {
        writer.write(((Number)o).longValue());
      } else if (o instanceof Float || o instanceof Double) {
        writer.write(((Number)o).doubleValue());
      } else {
        CharArr arr = new CharArr();
        arr.write(o.toString());
        writer.writeNumber(arr);
      }
    } else if (o instanceof Map) {
      this.serialize(writer, (Map)o);
    } else if (o instanceof Collection) {
      this.serialize(writer, (Collection)o);
    } else if (o instanceof Object[]) {
      this.serialize(writer, Arrays.asList(o));
    } else {
      writer.writeString(o.toString());
    }
  }
}
