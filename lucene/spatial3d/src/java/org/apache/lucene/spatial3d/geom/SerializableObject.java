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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

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

  /** Write a PlanetObject to a stream.
   * @param outputStream is the output stream.
   * @param object is the object to write.
   */
  public static void writePlanetObject(final OutputStream outputStream, final PlanetObject object) throws IOException {
    object.getPlanetModel().write(outputStream);
    writeObject(outputStream, object);
  }

  /** Read a PlanetObject from a stream.
   * @param inputStream is the input stream.
   * @return the PlanetObject.
   */
  public static PlanetObject readPlanetObject(final InputStream inputStream) throws IOException {
    final PlanetModel pm = new PlanetModel(inputStream);
    final SerializableObject so = readObject(pm, inputStream);
    if (!(so instanceof PlanetObject)) {
      throw new IOException("Type of object is not expected PlanetObject: "+so.getClass().getName());
    }
    return (PlanetObject)so;
  }

  /** Write an object to a stream.
   * @param outputStream is the output stream.
   * @param object is the object to write.
   */
  public static void writeObject(final OutputStream outputStream, final SerializableObject object) throws IOException {
    writeClass(outputStream, object.getClass());
    object.write(outputStream);
  }

  /** Read an object from a stream (for objects that need a PlanetModel).
   * @param planetModel is the planet model to use to deserialize the object.
   * @param inputStream is the input stream.
   * @return the deserialized object.
   */
  public static SerializableObject readObject(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    try {
      // Read the class
      final Class<?> clazz = readClass(inputStream);
      return readObject(planetModel, inputStream, clazz);
    } catch (ClassNotFoundException e) {
      throw new IOException("Can't find class for deserialization: "+e.getMessage(), e);
    }
  }

  /** Read an object from a stream (for objects that do not need a PlanetModel).
   * @param inputStream is the input stream.
   * @return the deserialized object.
   */
  public static SerializableObject readObject(final InputStream inputStream) throws IOException {
    try {
      // read the class
      final Class<?> clazz = readClass(inputStream);
      return readObject(inputStream, clazz);
    } catch (ClassNotFoundException e) {
      throw new IOException("Can't find class for deserialization: "+e.getMessage(), e);
    }
  }

  /** Instantiate a serializable object from a stream.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @param clazz is the class to instantiate.
   */
  static SerializableObject readObject(final PlanetModel planetModel, final InputStream inputStream, final Class<?> clazz) throws IOException {
    try {
      // Look for the right constructor
      final Constructor<?> c = clazz.getDeclaredConstructor(PlanetModel.class, InputStream.class);
      // Invoke it
      final Object object = c.newInstance(planetModel, inputStream);
      // check whether caste will work
      if (!(object instanceof SerializableObject)) {
        throw new IOException("Object "+clazz.getName()+" does not implement SerializableObject");
      }
      return (SerializableObject)object;
    } catch (InstantiationException e) {
      throw new IOException("Instantiation exception for class "+clazz.getName()+": "+e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IOException("Illegal access creating class "+clazz.getName()+": "+e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new IOException("No such method exception for class "+clazz.getName()+": "+e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IOException("Exception instantiating class "+clazz.getName()+": "+e.getMessage(), e);
    }

  }

  /** Instantiate a serializable object from a stream without a planet model.
   * @param inputStream is the input stream.
   * @param clazz is the class to instantiate.
   */
  static SerializableObject readObject(final InputStream inputStream, final Class<?> clazz) throws IOException {
    try {
      // Look for the right constructor
      final Constructor<?> c = clazz.getDeclaredConstructor(InputStream.class);
      // Invoke it
      final Object object = c.newInstance(inputStream);
      // check whether caste will work
      if (!(object instanceof SerializableObject)) {
        throw new IOException("Object "+clazz.getName()+" does not implement SerializableObject");
      }
      return (SerializableObject)object;
    } catch (InstantiationException e) {
      throw new IOException("Instantiation exception for class "+clazz.getName()+": "+e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new IOException("Illegal access creating class "+clazz.getName()+": "+e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new IOException("No such method exception for class "+clazz.getName()+": "+e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new IOException("Exception instantiating class "+clazz.getName()+": "+e.getMessage(), e);
    }

  }

  /** Write a class to a stream.
   * @param outputStream is the output stream.
   * @param clazz is the class to write.
   */
  static void writeClass(final OutputStream outputStream, final Class<?> clazz) throws IOException {
    Integer index = StandardObjects.classRegsitry.get(clazz);
    if (index == null){
      writeBoolean(outputStream, false);
      writeString(outputStream, clazz.getName());
    }
    else {
      writeBoolean(outputStream, true);
      outputStream.write(index);
    }
  }

  /**
   * Read the class from the stream
   * @param inputStream is the stream to read from.
   * @return is the class read
   */
  static Class<?> readClass(final InputStream inputStream) throws IOException, ClassNotFoundException {
    boolean standard = readBoolean(inputStream);
    if (standard) {
      int index = inputStream.read();
      return StandardObjects.codeRegsitry.get(index);
    }
    else {
      String className = readString(inputStream);
      return  Class.forName(className);
    }
  }

  /** Write a string to a stream.
   * @param outputStream is the output stream.
   * @param value is the string to write.
   */
  static void writeString(final OutputStream outputStream, final String value) throws IOException {
    writeByteArray(outputStream, value.getBytes(StandardCharsets.UTF_8));
  }

  /** Read a string from a stream.
   * @param inputStream is the stream to read from.
   * @return the string that was read.
   */
  static String readString(final InputStream inputStream) throws IOException {
    return new String(readByteArray(inputStream), StandardCharsets.UTF_8);
  }

  /** Write a point array.
   * @param outputStream is the output stream.
   * @param values is the array of points to write.
   */
  static void writePointArray(final OutputStream outputStream, final GeoPoint[] values) throws IOException {
    writeHomogeneousArray(outputStream, values);
  }

  /** Write a point array.
   * @param outputStream is the output stream.
   * @param values is the list of points to write.
   */
  static void writePointArray(final OutputStream outputStream, final List<GeoPoint> values) throws IOException {
    writeHomogeneousArray(outputStream, values);
  }

  /** Read a point array.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @return the array of points that was read.
   */
  static GeoPoint[] readPointArray(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    return readHomogeneousArray(planetModel, inputStream, GeoPoint.class);
  }

  /** Write a polgon array.
   * @param outputStream is the output stream.
   * @param values is the array of points to write.
   */
  static void writePolygonArray(final OutputStream outputStream, final GeoPolygon[] values) throws IOException {
    writeHeterogeneousArray(outputStream, values);
  }

  /** Write a polygon array.
   * @param outputStream is the output stream.
   * @param values is the list of points to write.
   */
  static void writePolygonArray(final OutputStream outputStream, final List<GeoPolygon> values) throws IOException {
    writeHeterogeneousArray(outputStream, values);
  }

  /** Read a polygon array.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @return the array of polygons that was read.
   */
  static GeoPolygon[] readPolygonArray(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    return readHeterogeneousArray(planetModel, inputStream, GeoPolygon.class);
  }

  /** Write an array.
   * @param outputStream is the output stream,.
   * @param values is the array.
   */
  static void writeHomogeneousArray(final OutputStream outputStream, final SerializableObject[] values) throws IOException {
    if (values == null) {
      writeInt(outputStream, 0);
    } else {
      writeInt(outputStream, values.length);
      for (final SerializableObject value : values) {
        value.write(outputStream);
      }
    }
  }

  /** Write an array.
   * @param outputStream is the output stream,.
   * @param values is the array.
   */
  static void writeHomogeneousArray(final OutputStream outputStream, final List<? extends SerializableObject> values) throws IOException {
    if (values == null) {
      writeInt(outputStream, 0);
    } else {
      writeInt(outputStream, values.size());
      for (final SerializableObject value : values) {
        value.write(outputStream);
      }
    }
  }

  /** Read an array.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @param clazz is the class of the objects to read.
   * @return the array.
   */
  static <T extends SerializableObject> T[] readHomogeneousArray(final PlanetModel planetModel, final InputStream inputStream, final Class<T> clazz) throws IOException {
    final int count = readInt(inputStream);
    @SuppressWarnings("unchecked")
    final T[] rval = (T[])Array.newInstance(clazz, count);
    for (int i = 0; i < count; i++) {
      rval[i] = clazz.cast(readObject(planetModel, inputStream, clazz));
    }
    return rval;
  }

  /** Write an array.
   * @param outputStream is the output stream,.
   * @param values is the array.
   */
  static void writeHeterogeneousArray(final OutputStream outputStream, final SerializableObject[] values) throws IOException {
    if (values == null) {
      writeInt(outputStream, 0);
    } else {
      writeInt(outputStream, values.length);
      for (final SerializableObject value : values) {
        writeObject(outputStream, value);
      }
    }
  }

  /** Write an array.
   * @param outputStream is the output stream,.
   * @param values is the array.
   */
  static void writeHeterogeneousArray(final OutputStream outputStream, final List<? extends SerializableObject> values) throws IOException {
    if (values == null) {
      writeInt(outputStream, 0);
    } else {
      writeInt(outputStream, values.size());
      for (final SerializableObject value : values) {
        writeObject(outputStream, value);
      }
    }
  }

  /** Read an array.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @return the array.
   */
  static <T extends SerializableObject> T[] readHeterogeneousArray(final PlanetModel planetModel, final InputStream inputStream, final Class<T> clazz) throws IOException {
    final int count = readInt(inputStream);
    @SuppressWarnings("unchecked")
    final T[] rval = (T[])Array.newInstance(clazz, count);
    for (int i = 0; i < count; i++) {
      rval[i] = clazz.cast(readObject(planetModel, inputStream));
    }
    return rval;
  }

  /** Write a bitset to a stream.
   * @param outputStream is the output stream.
   * @param bitSet is the bit set to write.
   */
  static void writeBitSet(final OutputStream outputStream, final BitSet bitSet) throws IOException {
    writeByteArray(outputStream, bitSet.toByteArray());
  }

  /** Read a bitset from a stream.
   * @param inputStream is the input stream.
   * @return the bitset read from the stream.
   */
  static BitSet readBitSet(final InputStream inputStream) throws IOException {
    return BitSet.valueOf(readByteArray(inputStream));
  }

  /** Write byte array.
   * @param outputStream is the output stream.
   * @param bytes is the byte array.
   */
  static void writeByteArray(final OutputStream outputStream, final byte[] bytes) throws IOException {
    writeInt(outputStream, bytes.length);
    outputStream.write(bytes);
  }

  /** Read byte array.
   * @param inputStream is the input stream.
   * @return the byte array.
   */
  static byte[] readByteArray(final InputStream inputStream) throws IOException {
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
    return bytes;
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

  /** Write a boolean to a stream.
   * @param outputStream is the output stream.
   * @param value is the value to write.
   */
  static void writeBoolean(final OutputStream outputStream, final boolean value) throws IOException {
    outputStream.write(value?1:0);
  }

  /** Read a boolean from a stream.
   * @param inputStream is the input stream.
   * @return the boolean value.
   */
  static boolean readBoolean(final InputStream inputStream) throws IOException {
    final int valueRead = inputStream.read();
    if (valueRead == -1) {
      throw new IOException("Unexpected end of input stream");
    }
    return (valueRead == 0)?false:true;
  }

}

