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
package org.apache.solr.handler.clustering;

import org.carrot2.attrs.AcceptingVisitor;
import org.carrot2.attrs.AliasMapper;
import org.carrot2.attrs.AttrBoolean;
import org.carrot2.attrs.AttrDouble;
import org.carrot2.attrs.AttrEnum;
import org.carrot2.attrs.AttrInteger;
import org.carrot2.attrs.AttrObject;
import org.carrot2.attrs.AttrObjectArray;
import org.carrot2.attrs.AttrString;
import org.carrot2.attrs.AttrStringArray;
import org.carrot2.attrs.AttrVisitor;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link AttrVisitor} that responds to "flattened" key paths and values, updating
 * corresponding algorithm parameters with values contained in the map.
 */
class FlatKeysAttrVisitor implements AttrVisitor {
  final Function<String, Object> classToInstance = AliasMapper.SPI_DEFAULTS::fromName;
  final ArrayDeque<String> keyPath = new ArrayDeque<>();

  final LinkedHashMap<String, String> attrs;

  /**
   * @param attrs A map of attributes to set. Note the map has ordered keys:
   *              this is required for complex sub-types so that instantiation of
   *              a value precedes setting its attributes.
   */
  FlatKeysAttrVisitor(LinkedHashMap<String, String> attrs) {
    this.attrs = attrs;
  }

  @Override
  public void visit(String key, AttrBoolean attr) {
    ifKeyExists(key, (path, value) -> {
      attr.set(value == null ? null : Boolean.parseBoolean(value));
    });
  }

  @Override
  public void visit(String key, AttrInteger attr) {
    ifKeyExists(key, (path, value) -> {
      attr.set(value == null ? null : Integer.parseInt(value));
    });
  }

  @Override
  public void visit(String key, AttrDouble attr) {
    ifKeyExists(key, (path, value) -> {
      attr.set(value == null ? null : Double.parseDouble(value));
    });
  }

  @Override
  public void visit(String key, AttrString attr) {
    ifKeyExists(key, (path, value) -> {
      attr.set(value);
    });
  }

  @Override
  public void visit(String key, AttrStringArray attr) {
    ifKeyExists(key, (path, value) -> {
      if (value == null) {
        attr.set(new String[0]);
      } else {
        attr.set(value.split(",\\s*"));
      }
    });
  }

  @Override
  public <T extends Enum<T>> void visit(String key, AttrEnum<T> attr) {
    ifKeyExists(key, (path, value) -> {
      try {
        attr.set(Enum.valueOf(attr.enumClass(), value));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Value at key '%s' should be an enum constant of class '%s', but no such " +
                    "constant exists: '%s' (available constants: %s)",
                key,
                attr.enumClass().getSimpleName(),
                toDebugString(value),
                EnumSet.allOf(attr.enumClass())));
      }
    });
  }

  @Override
  public <T extends AcceptingVisitor> void visit(String key, AttrObject<T> attr) {
    ifKeyExists(key, (path, value) -> {
      if (value == null) {
        attr.set(null);
      } else {
        T t = safeCast(classToInstance.apply(value), key, attr.getInterfaceClass());
        attr.set(t);
      }
    });

    T t = attr.get();
    if (t != null) {
      withKey(key, path -> {
        t.accept(this);
      });
    }
  }

  @Override
  public <T extends AcceptingVisitor> void visit(String key, AttrObjectArray<T> attr) {
    ifKeyExists(key, (path, value) -> {
      throw new RuntimeException("Setting arrays of objects not implemented for attribute: "
          + key + " (" + attr.getDescription() + ")");
    });
  }

  private <T> T safeCast(Object value, String key, Class<T> clazz) {
    if (value == null) {
      return null;
    } else {
      if (!clazz.isInstance(value)) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Value at key '%s' should be an instance of '%s', but encountered class '%s': '%s'",
                key,
                clazz.getSimpleName(),
                value.getClass().getSimpleName(),
                toDebugString(value)));
      }
      return clazz.cast(value);
    }
  }

  private String toDebugString(Object value) {
    if (value == null) {
      return "[null]";
    } else if (value instanceof Object[]) {
      return Arrays.deepToString(((Object[]) value));
    } else {
      return Objects.toString(value);
    }
  }

  private void withKey(String key, Consumer<String> pathConsumer) {
    keyPath.addLast(key);
    try {
      String path = String.join(".", keyPath);
      pathConsumer.accept(path);
    } finally {
      keyPath.removeLast();
    }
  }

  private void ifKeyExists(String key, BiConsumer<String, String> pathConsumer) {
    withKey(key, (path) -> {
      if (attrs.containsKey(path)) {
        String value = attrs.get(path);
        if (value.trim().isEmpty()) {
          value = null;
        }
        pathConsumer.accept(path, value);
      }
    });
  }
}
