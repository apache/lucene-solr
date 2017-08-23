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

package org.apache.lucene.spatial3d.geom;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Indicates that a geo3d object can be serialized and deserialized.
 *
 * @lucene.experimental
 */
public interface SerializableObject {

  /** Serialize to output stream.
   * @param outputStream is the output stream to write to.
   */
  void write(OutputStream outputStream) throws IOException;

  /** Write an object to a stream.
   * @param outputStream is the output stream.
   * @param object is the object to write.
   */
  static void writeObject(final OutputStream outputStream, final SerializableObject object) throws IOException {
    writeString(outputStream, object.getClass().getName());
    object.write(outputStream);
  }
  
  /** Read an object from a stream (for objects that need a PlanetModel).
   * @param planetModel is the planet model to use to deserialize the object.
   * @param inputStream is the input stream.
   * @return the deserialized object.
   */
  static SerializableObject readObject(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    // Read the class name
    final String className = readString(inputStream);
    try {
      // Look for the class
      final Class<?> clazz = Class.forName(className);
      // Look for the right constructor
      final Constructor<?> c = clazz.getDeclaredConstructor(PlanetModel.class, InputStream.class);
      // Invoke it
      final Object object = c.newInstance(planetModel, inputStream);
      // check whether caste will work
      if (!(object instanceof SerializableObject)) {
        throw new IOException("Object "+className+" does not implement SerializableObject");
      }
      return (SerializableObject)object;
    } catch (ClassNotFoundException e) {
      throw new IOException("Can't find class "+className+" for deserialization: "+e.getMessage(), e);
    } catch (InstantiationException e) {
      throw new IOException("Instantiation exception for class "+className+": "+e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IOException("Illegal access creating class "+className+": "+e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new IOException("No such method exception for class "+className+": "+e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IOException("Exception instantiating class "+className+": "+e.getMessage(), e);
    }
  }

  /** Read an object from a stream (for objects that do not need a PlanetModel).
   * @param inputStream is the input stream.
   * @return the deserialized object.
   */
  static SerializableObject readObject(final InputStream inputStream) throws IOException {
    // Read the class name
    final String className = readString(inputStream);
    try {
      // Look for the class
      final Class<?> clazz = Class.forName(className);
      // Look for the right constructor
      final Constructor<?> c = clazz.getDeclaredConstructor(InputStream.class);
      // Invoke it
      final Object object = c.newInstance(inputStream);
      // check whether caste will work
      if (!(object instanceof SerializableObject)) {
        throw new IOException("Object "+className+" does not implement SerializableObject");
      }
      return (SerializableObject)object;
    } catch (ClassNotFoundException e) {
      throw new IOException("Can't find class "+className+" for deserialization: "+e.getMessage(), e);
    } catch (InstantiationException e) {
      throw new IOException("Instantiation exception for class "+className+": "+e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IOException("Illegal access creating class "+className+": "+e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new IOException("No such method exception for class "+className+": "+e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IOException("Exception instantiating class "+className+": "+e.getMessage(), e);
    }
  }
  
  /** Write a string to a stream.
   * @param outputStream is the output stream.
   * @param value is the string to write.
   */
  static void writeString(final OutputStream outputStream, final String value) throws IOException {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeInt(outputStream, bytes.length);
    outputStream.write(bytes);
  }
  
  /** Read a string from a stream.
   * @param inputStream is the stream to read from.
   * @return the string that was read.
   */
  static String readString(final InputStream inputStream) throws IOException {
    int stringLength = readInt(inputStream);
    int stringOffset = 0;
    final byte[] bytes = new byte[stringLength];
    while (stringLength > 0) {
      final int amt = inputStream.read(bytes, stringOffset, stringLength);
      if (amt == -1) {
        throw new IOException("Unexpected end of input stream");
      }
      stringOffset += amt;
      stringLength -= amt;
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }
  
  /** Write a double to a stream.
   * @param outputStream is the output stream.
   * @param value is the value to write.
   */
  static void writeDouble(final OutputStream outputStream, final double value) throws IOException {
    writeLong(outputStream, Double.doubleToLongBits(value));
  }
  
  /** Read a double from a stream.
   * @param inputStream is the input stream.
   * @return the double value read from the stream.
   */
  static double readDouble(final InputStream inputStream) throws IOException {
    return Double.longBitsToDouble(readLong(inputStream));
  }
  
  /** Write a long to a stream.
   * @param outputStream is the output stream.
   * @param value is the value to write.
   */
  static void writeLong(final OutputStream outputStream, final long value) throws IOException {
    writeInt(outputStream, (int)value);
    writeInt(outputStream, (int)(value >> 32));
  }
  
  /** Read a long from a stream.
   * @param inputStream is the input stream.
   * @return the long value read from the stream.
   */
  static long readLong(final InputStream inputStream) throws IOException {
    final long lower = ((long)(readInt(inputStream))) & 0x00000000ffffffffL;
    final long upper = (((long)(readInt(inputStream))) << 32) & 0xffffffff00000000L;
    return lower + upper;
  }
  
  /** Write an int to a stream.
   * @param outputStream is the output stream.
   * @param value is the value to write.
   */
  static void writeInt(final OutputStream outputStream, final int value) throws IOException {
    outputStream.write(value);
    outputStream.write(value >> 8);
    outputStream.write(value >> 16);
    outputStream.write(value >> 24);
  }
  
  /** Read an int from a stream.
   * @param inputStream is the input stream.
   * @return the value read from the stream.
   */
  static int readInt(final InputStream inputStream) throws IOException {
    final int l1 = (inputStream.read()) & 0x000000ff;
    final int l2 = (inputStream.read() << 8) & 0x0000ff00;
    final int l3 = (inputStream.read() << 16) & 0x00ff0000;
    final int l4 = (inputStream.read() << 24) & 0xff000000;
    return l1 + l2 + l3 + l4;
  }

}
