package org.apache.lucene.util;

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

import static org.apache.lucene.util.RamUsageEstimator.*;

import java.util.Random;

public class TestRamUsageEstimator extends LuceneTestCase {
  public void testSanity() {
    assertTrue(sizeOf(new String("test string")) > shallowSizeOfInstance(String.class));

    Holder holder = new Holder();
    holder.holder = new Holder("string2", 5000L);
    assertTrue(sizeOf(holder) > shallowSizeOfInstance(Holder.class));
    assertTrue(sizeOf(holder) > sizeOf(holder.holder));
    
    assertTrue(
        shallowSizeOfInstance(HolderSubclass.class) >= shallowSizeOfInstance(Holder.class));
    assertTrue(
        shallowSizeOfInstance(Holder.class)         == shallowSizeOfInstance(HolderSubclass2.class));

    String[] strings = new String[] {
        new String("test string"),
        new String("hollow"), 
        new String("catchmaster")
    };
    assertTrue(sizeOf(strings) > shallowSizeOf(strings));
  }

  public void testStaticOverloads() {
    Random rnd = random();
    {
      byte[] array = new byte[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      boolean[] array = new boolean[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      char[] array = new char[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      short[] array = new short[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      int[] array = new int[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      float[] array = new float[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      long[] array = new long[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      double[] array = new double[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
  }
  
  public void testReferenceSize() {
    if (!isSupportedJVM()) {
      System.err.println("WARN: Your JVM does not support certain Oracle/Sun extensions.");
      System.err.println(" Memory estimates may be inaccurate.");
      System.err.println(" Please report this to the Lucene mailing list.");
      System.err.println("JVM version: " + RamUsageEstimator.JVM_INFO_STRING);
      System.err.println("UnsupportedFeatures:");
      for (JvmFeature f : RamUsageEstimator.getUnsupportedFeatures()) {
        System.err.print(" - " + f.toString());
        if (f == RamUsageEstimator.JvmFeature.OBJECT_ALIGNMENT) {
          System.err.print("; Please note: 32bit Oracle/Sun VMs don't allow exact OBJECT_ALIGNMENT retrieval, this is a known issue.");
        }
        System.err.println();
      }
    }

    assertTrue(NUM_BYTES_OBJECT_REF == 4 || NUM_BYTES_OBJECT_REF == 8);
    if (!Constants.JRE_IS_64BIT) {
      assertEquals("For 32bit JVMs, reference size must always be 4?", 4, NUM_BYTES_OBJECT_REF);
    }
  }

  @SuppressWarnings("unused")
  private static class Holder {
    long field1 = 5000L;
    String name = "name";
    Holder holder;
    long field2, field3, field4;
    
    Holder() {}
    
    Holder(String name, long field1) {
      this.name = name;
      this.field1 = field1;
    }
  }
  
  @SuppressWarnings("unused")
  private static class HolderSubclass extends Holder {
    byte foo;
    int bar;
  }
  
  private static class HolderSubclass2 extends Holder {
    // empty, only inherits all fields -> size should be identical to superclass
  }
}
