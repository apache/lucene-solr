package org.apache.lucene.index.values;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IndexDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SlowMultiReaderWrapper;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.lucene40.values.BytesRefUtils;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TestTypePromotion extends LuceneTestCase {
  @Before
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("cannot work with preflex codec", Codec.getDefault().getName().equals("Lucene3x"));
  }

  private static EnumSet<ValueType> INTEGERS = EnumSet.of(ValueType.VAR_INTS,
      ValueType.FIXED_INTS_16, ValueType.FIXED_INTS_32,
      ValueType.FIXED_INTS_64, ValueType.FIXED_INTS_8);

  private static EnumSet<ValueType> FLOATS = EnumSet.of(ValueType.FLOAT_32,
      ValueType.FLOAT_64);

  private static EnumSet<ValueType> UNSORTED_BYTES = EnumSet.of(
      ValueType.BYTES_FIXED_DEREF, ValueType.BYTES_FIXED_STRAIGHT,
      ValueType.BYTES_VAR_STRAIGHT, ValueType.BYTES_VAR_DEREF);

  private static EnumSet<ValueType> SORTED_BYTES = EnumSet.of(
      ValueType.BYTES_FIXED_SORTED, ValueType.BYTES_VAR_SORTED);
  
  public ValueType randomValueType(EnumSet<ValueType> typeEnum, Random random) {
    ValueType[] array = typeEnum.toArray(new ValueType[0]);
    return array[random.nextInt(array.length)];
  }
  
  private static enum TestType {
    Int, Float, Byte
  }

  private void runTest(EnumSet<ValueType> types, TestType type)
      throws CorruptIndexException, IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    int num_1 = atLeast(200);
    int num_2 = atLeast(200);
    int num_3 = atLeast(200);
    long[] values = new long[num_1 + num_2 + num_3];
    index(writer, new IndexDocValuesField("promote"),
        randomValueType(types, random), values, 0, num_1);
    writer.commit();
    
    index(writer, new IndexDocValuesField("promote"),
        randomValueType(types, random), values, num_1, num_2);
    writer.commit();
    
    if (random.nextInt(4) == 0) {
      // once in a while use addIndexes
      writer.forceMerge(1);
      
      Directory dir_2 = newDirectory() ;
      IndexWriter writer_2 = new IndexWriter(dir_2,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      index(writer_2, new IndexDocValuesField("promote"),
          randomValueType(types, random), values, num_1 + num_2, num_3);
      writer_2.commit();
      writer_2.close();
      if (random.nextBoolean()) {
        writer.addIndexes(dir_2);
      } else {
        // do a real merge here
        IndexReader open = IndexReader.open(dir_2);
        // we cannot use SlowMR for sorted bytes, because it returns a null sortedsource
        boolean useSlowMRWrapper = types != SORTED_BYTES && random.nextBoolean();
        writer.addIndexes(useSlowMRWrapper ? new SlowMultiReaderWrapper(open) : open);
        open.close();
      }
      dir_2.close();
    } else {
      index(writer, new IndexDocValuesField("promote"),
          randomValueType(types, random), values, num_1 + num_2, num_3);
    }

    writer.forceMerge(1);
    writer.close();
    assertValues(type, dir, values);
    dir.close();
  }

  
  private void assertValues(TestType type, Directory dir, long[] values)
      throws CorruptIndexException, IOException {
    IndexReader reader = IndexReader.open(dir);
    assertEquals(1, reader.getSequentialSubReaders().length);
    ReaderContext topReaderContext = reader.getTopReaderContext();
    ReaderContext[] children = topReaderContext.children();
    IndexDocValues docValues = children[0].reader.docValues("promote");
    assertEquals(1, children.length);
    Source directSource = docValues.getDirectSource();
    for (int i = 0; i < values.length; i++) {
      int id = Integer.parseInt(reader.document(i).get("id"));
      String msg = "id: " + id + " doc: " + i;
      switch (type) {
      case Byte:
        BytesRef bytes = directSource.getBytes(i, new BytesRef());
        long value = 0;
        switch(bytes.length) {
        case 1:
          value = bytes.bytes[bytes.offset];
          break;
        case 2:
          value = BytesRefUtils.asShort(bytes);
          break;
        case 4:
          value = BytesRefUtils.asInt(bytes);
          break;
        case 8:
          value = BytesRefUtils.asLong(bytes);
          break;
          
        default:
          fail(msg + " bytessize: " + bytes.length);
        }
        
        assertEquals(msg  + " byteSize: " + bytes.length, values[id], value);
        break;
      case Float:
          assertEquals(msg, values[id], Double.doubleToRawLongBits(directSource.getFloat(i)));
        break;
      case Int:
        assertEquals(msg, values[id], directSource.getInt(i));
        break;
      default:
        break;
      }

    }
    docValues.close();
    reader.close();
  }

  public void index(IndexWriter writer, IndexDocValuesField valField,
      ValueType valueType, long[] values, int offset, int num)
      throws CorruptIndexException, IOException {
    BytesRef ref = new BytesRef(new byte[] { 1, 2, 3, 4 });
    for (int i = offset; i < offset + num; i++) {
      Document doc = new Document();
      doc.add(new Field("id", i + "", TextField.TYPE_STORED));
      switch (valueType) {
      case VAR_INTS:
        values[i] = random.nextInt();
        valField.setInt(values[i]);
        break;
      case FIXED_INTS_16:
        values[i] = random.nextInt(Short.MAX_VALUE);
        valField.setInt((short) values[i], true);
        break;
      case FIXED_INTS_32:
        values[i] = random.nextInt();
        valField.setInt((int) values[i], true);
        break;
      case FIXED_INTS_64:
        values[i] = random.nextLong();
        valField.setInt(values[i], true);
        break;
      case FLOAT_64:
        double nextDouble = random.nextDouble();
        values[i] = Double.doubleToRawLongBits(nextDouble);
        valField.setFloat(nextDouble);
        break;
      case FLOAT_32:
        final float nextFloat = random.nextFloat();
        values[i] = Double.doubleToRawLongBits(nextFloat);
        valField.setFloat(nextFloat);
        break;
      case FIXED_INTS_8:
         values[i] = (byte) i;
        valField.setInt((byte)values[i], true);
        break;
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
        values[i] = random.nextLong();
        BytesRefUtils.copyLong(ref, values[i]);
        valField.setBytes(ref, valueType);
        break;
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        if (random.nextBoolean()) {
          BytesRefUtils.copyInt(ref, random.nextInt());
          values[i] = BytesRefUtils.asInt(ref);
        } else {
          BytesRefUtils.copyLong(ref, random.nextLong());
          values[i] = BytesRefUtils.asLong(ref);
        }
        valField.setBytes(ref, valueType);
        break;

      default:
        fail("unexpected value " + valueType);

      }
      doc.add(valField);
      writer.addDocument(doc);
      if (random.nextInt(10) == 0) {
        writer.commit();
      }
    }
  }

  public void testPromoteBytes() throws IOException {
    runTest(UNSORTED_BYTES, TestType.Byte);
  }
  
  public void testSortedPromoteBytes() throws IOException {
    runTest(SORTED_BYTES, TestType.Byte);
  }

  public void testPromotInteger() throws IOException {
    runTest(INTEGERS, TestType.Int);
  }

  public void testPromotFloatingPoint() throws CorruptIndexException,
      IOException {
    runTest(FLOATS, TestType.Float);
  }
  
  public void testMergeIncompatibleTypes() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig writerConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    writerConfig.setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES); // no merges until we are done with adding values
    IndexWriter writer = new IndexWriter(dir, writerConfig);
    int num_1 = atLeast(200);
    int num_2 = atLeast(200);
    long[] values = new long[num_1 + num_2];
    index(writer, new IndexDocValuesField("promote"),
        randomValueType(INTEGERS, random), values, 0, num_1);
    writer.commit();
    
    if (random.nextInt(4) == 0) {
      // once in a while use addIndexes
      Directory dir_2 = newDirectory() ;
      IndexWriter writer_2 = new IndexWriter(dir_2,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      index(writer_2, new IndexDocValuesField("promote"),
          randomValueType(random.nextBoolean() ? UNSORTED_BYTES : SORTED_BYTES, random), values, num_1, num_2);
      writer_2.commit();
      writer_2.close();
      if (random.nextBoolean()) {
        writer.addIndexes(dir_2);
      } else {
        // do a real merge here
        IndexReader open = IndexReader.open(dir_2);
        writer.addIndexes(open);
        open.close();
      }
      dir_2.close();
    } else {
      index(writer, new IndexDocValuesField("promote"),
          randomValueType(random.nextBoolean() ? UNSORTED_BYTES : SORTED_BYTES, random), values, num_1, num_2);
      writer.commit();
    }
    writer.close();
    writerConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    if (writerConfig.getMergePolicy() instanceof NoMergePolicy) {
      writerConfig.setMergePolicy(newLogMergePolicy()); // make sure we merge to one segment (merge everything together)
    }
    writer = new IndexWriter(dir, writerConfig);
    // now merge
    writer.forceMerge(1);
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    assertEquals(1, reader.getSequentialSubReaders().length);
    ReaderContext topReaderContext = reader.getTopReaderContext();
    ReaderContext[] children = topReaderContext.children();
    IndexDocValues docValues = children[0].reader.docValues("promote");
    assertNotNull(docValues);
    assertValues(TestType.Byte, dir, values);
    assertEquals(ValueType.BYTES_VAR_STRAIGHT, docValues.type());
    reader.close();
    dir.close();
  }

}