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

/**
 * This represents a data entry in the payload/stream. There are multiple ways to consume the data entry
 * a) listen to it, if it's a container object, and get callbacks for each sub-entry
 * b) read as an object using the {{@link #val()}} method. Please note that it creates objects and expect more memory usage
 * c) read the corresponding primitive value
 * Do not keep a reference of this Object beyond the scope where it is called. Read the relevant data out.
 */
public interface DataEntry {
  /**
   * The data type
   */
  DataEntry.Type type();

  /**
   * The index of this entry in the container
   */
  long index();

  int intVal();

  long longVal();

  float floatVal();

  double doubleVal();

  boolean boolVal();

  default String strValue() {
    if (type() == null) return null;
    return val().toString();
  }

  /**
   * The object value
   */
  Object val();

  /**
   * Register a listener to get callbacks for all entries
   *
   * @param ctx      This is any object that should be shared with the child entry callbacks
   * @param listener The listener that handles each entry in this container
   */
  void listenContainer(Object ctx, EntryListener listener);

  /**
   * Some Objects may have metadata. usually there is none
   */

  Object metadata();

  /**Depth of this Object. The root most object has a depth of 1
   */
  int depth();

  /**
   * If this is a child of another container object this returns a non-null value
   *
   * @return the parent container object
   */
  DataEntry parent();

  /**
   * This is the object shared in the parent container in the {{@link #listenContainer(Object, EntryListener)}} method
   */
  Object ctx();

  /**
   * If it is a non-primitive type type and size is known in advance
   *
   * if it's a map/list, it's the no:of items in this container
   *
   * if it's a {{@link CharSequence}} or byte[] , it's the no:of bytes in the stream
   *
   * @return a number greater than or equal to zero if the size is known, -1 if unknown
   */
  int length();

  /**
   * If this object is a key value entry. key value entries have name
   */
  boolean isKeyValEntry();

  /**
   * The name, if this is a map entry , else it returns a null
   */
  CharSequence name();

  /**
   * The types are a superset of json
   */
  enum Type {
    NULL(true),
    LONG(true),
    INT(true),
    BOOL(true),
    FLOAT(true),
    DOUBLE(true),
    DATE(true),
    /**
     * A map like json object
     */
    KEYVAL_ITER(false, true),
    /**
     * An array like json object
     */
    ENTRY_ITER(false, true),
    STR(false),
    BYTEARR(false),
    /**
     * don't know how to stream it. read as an object using {{@link DataEntry#val()}} method
     */
    JAVA_OBJ(false);
    /**
     * A primitive type which usually maps to a java primitive
     */
    public final boolean isPrimitive;

    public final boolean isContainer;

    Type(boolean isPrimitive) {
      this(isPrimitive, false);
    }

    Type(boolean isPrimitive, boolean isContainer) {
      this.isPrimitive = isPrimitive;
      this.isContainer = isContainer;
    }
  }

  interface EntryListener {

    /**
     * Callback for each entry in this container. once the method call returns, the entry object is not valid anymore
     * It is usually reused.
     * If the object value is a {{@link Utf8CharSequence}} do a {{@link Object#clone()}} because the object may be reused
     *
     * @param e The entry in the container
     */
    void entry(DataEntry e);

    /**
     * Callback after all entries of this container are streamed
     *
     * @param e the container entry
     */
    default void end(DataEntry e) {
    }
  }

  interface FastDecoder {

    FastDecoder withInputStream(InputStream is);

    Object decode(EntryListener iterListener) throws IOException;

  }
}
