package org.apache.lucene;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util._TestUtil;

import java.util.Date;
import java.util.Random;

class StoreTest {
  public static void main(String[] args) {
    try {
      test(1000, true, true);
    } catch (Exception e) {
		e.printStackTrace();
    }
  }

  public static void test(int count, boolean ram, boolean buffered)
       throws Exception {
    Random gen = new Random(1251971);
    int i;
    
    Date veryStart = new Date();
    Date start = new Date();

    Directory store;
    if (ram)
      store = new RAMDirectory();
    else {
      String dirName = "test.store";
      _TestUtil.rmDir(dirName);
      store = FSDirectory.getDirectory(dirName);
    }

    final int LENGTH_MASK = 0xFFF;

	final byte[] buffer = new byte[LENGTH_MASK];

    for (i = 0; i < count; i++) {
      String name = i + ".dat";
      int length = gen.nextInt() & LENGTH_MASK;
      byte b = (byte)(gen.nextInt() & 0x7F);
      //System.out.println("filling " + name + " with " + length + " of " + b);

      IndexOutput file = store.createOutput(name);

      if (buffered) {
        for (int j = 0; j < length; j++)
          buffer[j] = b;
        file.writeBytes(buffer, length);
      } else {
        for (int j = 0; j < length; j++)
          file.writeByte(b);
      }
      
      file.close();
    }

    store.close();

    Date end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds to create");

    gen = new Random(1251971);
    start = new Date();

    if (!ram)
      store = FSDirectory.getDirectory("test.store");

    for (i = 0; i < count; i++) {
      String name = i + ".dat";
      int length = gen.nextInt() & LENGTH_MASK;
      byte b = (byte)(gen.nextInt() & 0x7F);
      //System.out.println("reading " + name + " with " + length + " of " + b);

      IndexInput file = store.openInput(name);

      if (file.length() != length)
	throw new Exception("length incorrect");

      byte[] content = new byte[length];
      if (buffered) {
        file.readBytes(content, 0, length);
        // check the buffer
        for (int j = 0; j < length; j++)
          if (content[j] != b)
            throw new Exception("contents incorrect");
      } else {
        for (int j = 0; j < length; j++)
          if (file.readByte() != b)
            throw new Exception("contents incorrect");
      }

      file.close();
    }

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds to read");

    gen = new Random(1251971);
    start = new Date();

    for (i = 0; i < count; i++) {
      String name = i + ".dat";
      //System.out.println("deleting " + name);
      store.deleteFile(name);
    }

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds to delete");

    System.out.print(end.getTime() - veryStart.getTime());
    System.out.println(" total milliseconds");

    store.close();
  }
}
