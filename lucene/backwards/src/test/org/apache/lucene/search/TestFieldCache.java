package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class TestFieldCache extends LuceneTestCase {
  protected IndexReader reader;
  private static final int NUM_DOCS = atLeast(1000);
  private Directory directory;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random, directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    byte theByte = Byte.MAX_VALUE;
    short theShort = Short.MAX_VALUE;
    int theInt = Integer.MAX_VALUE;
    float theFloat = Float.MAX_VALUE;
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(newField("theLong", String.valueOf(theLong--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(newField("theDouble", String.valueOf(theDouble--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(newField("theByte", String.valueOf(theByte--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(newField("theShort", String.valueOf(theShort--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(newField("theInt", String.valueOf(theInt--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(newField("theFloat", String.valueOf(theFloat--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();
    reader = IndexReader.open(directory, true);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  public void testInfoStream() throws Exception {
    try {
      FieldCache cache = FieldCache.DEFAULT;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      cache.setInfoStream(new PrintStream(bos));
      cache.getDoubles(reader, "theDouble");
      cache.getFloats(reader, "theDouble");
      assertTrue(bos.toString().indexOf("WARNING") != -1);
    } finally {
      FieldCache.DEFAULT.purgeAllCaches();
    }
  }

  public void test() throws IOException {
    FieldCache cache = FieldCache.DEFAULT;
    double [] doubles = cache.getDoubles(reader, "theDouble");
    assertSame("Second request to cache return same array", doubles, cache.getDoubles(reader, "theDouble"));
    assertSame("Second request with explicit parser return same array", doubles, cache.getDoubles(reader, "theDouble", FieldCache.DEFAULT_DOUBLE_PARSER));
    assertTrue("doubles Size: " + doubles.length + " is not: " + NUM_DOCS, doubles.length == NUM_DOCS);
    for (int i = 0; i < doubles.length; i++) {
      assertTrue(doubles[i] + " does not equal: " + (Double.MAX_VALUE - i), doubles[i] == (Double.MAX_VALUE - i));

    }
    
    long [] longs = cache.getLongs(reader, "theLong");
    assertSame("Second request to cache return same array", longs, cache.getLongs(reader, "theLong"));
    assertSame("Second request with explicit parser return same array", longs, cache.getLongs(reader, "theLong", FieldCache.DEFAULT_LONG_PARSER));
    assertTrue("longs Size: " + longs.length + " is not: " + NUM_DOCS, longs.length == NUM_DOCS);
    for (int i = 0; i < longs.length; i++) {
      assertTrue(longs[i] + " does not equal: " + (Long.MAX_VALUE - i), longs[i] == (Long.MAX_VALUE - i));

    }
    
    byte [] bytes = cache.getBytes(reader, "theByte");
    assertSame("Second request to cache return same array", bytes, cache.getBytes(reader, "theByte"));
    assertSame("Second request with explicit parser return same array", bytes, cache.getBytes(reader, "theByte", FieldCache.DEFAULT_BYTE_PARSER));
    assertTrue("bytes Size: " + bytes.length + " is not: " + NUM_DOCS, bytes.length == NUM_DOCS);
    for (int i = 0; i < bytes.length; i++) {
      assertTrue(bytes[i] + " does not equal: " + (Byte.MAX_VALUE - i), bytes[i] == (byte) (Byte.MAX_VALUE - i));

    }
    
    short [] shorts = cache.getShorts(reader, "theShort");
    assertSame("Second request to cache return same array", shorts, cache.getShorts(reader, "theShort"));
    assertSame("Second request with explicit parser return same array", shorts, cache.getShorts(reader, "theShort", FieldCache.DEFAULT_SHORT_PARSER));
    assertTrue("shorts Size: " + shorts.length + " is not: " + NUM_DOCS, shorts.length == NUM_DOCS);
    for (int i = 0; i < shorts.length; i++) {
      assertTrue(shorts[i] + " does not equal: " + (Short.MAX_VALUE - i), shorts[i] == (short) (Short.MAX_VALUE - i));

    }
    
    int [] ints = cache.getInts(reader, "theInt");
    assertSame("Second request to cache return same array", ints, cache.getInts(reader, "theInt"));
    assertSame("Second request with explicit parser return same array", ints, cache.getInts(reader, "theInt", FieldCache.DEFAULT_INT_PARSER));
    assertTrue("ints Size: " + ints.length + " is not: " + NUM_DOCS, ints.length == NUM_DOCS);
    for (int i = 0; i < ints.length; i++) {
      assertTrue(ints[i] + " does not equal: " + (Integer.MAX_VALUE - i), ints[i] == (Integer.MAX_VALUE - i));

    }
    
    float [] floats = cache.getFloats(reader, "theFloat");
    assertSame("Second request to cache return same array", floats, cache.getFloats(reader, "theFloat"));
    assertSame("Second request with explicit parser return same array", floats, cache.getFloats(reader, "theFloat", FieldCache.DEFAULT_FLOAT_PARSER));
    assertTrue("floats Size: " + floats.length + " is not: " + NUM_DOCS, floats.length == NUM_DOCS);
    for (int i = 0; i < floats.length; i++) {
      assertTrue(floats[i] + " does not equal: " + (Float.MAX_VALUE - i), floats[i] == (Float.MAX_VALUE - i));

    }
  }
}
