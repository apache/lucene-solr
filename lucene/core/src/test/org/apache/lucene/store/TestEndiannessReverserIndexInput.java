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
package org.apache.lucene.store;

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestEndiannessReverserIndexInput extends LuceneTestCase {
    
    public void testReadShort() throws IOException {
        Directory directory = newDirectory();
        IndexOutput output = directory.createOutput("endianness", IOContext.DEFAULT);
        int values = atLeast(30);
        boolean useHelper = random().nextBoolean();
        for (int i = 0; i < values; i++) {
            if (useHelper) {
                EndiannessReverserUtil.writeShort(output, (short) random().nextInt());
            } else {
                output.writeShort((short) random().nextInt());
            }
        }
        long len = output.getFilePointer();
        output.close();
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            for (int i = 0; i < values; i++) {
                assertEquals(input.readShort(), EndiannessReverserUtil.readShort(wrapped));
            }
            input.close();
            wrapped.close();
        }
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            IndexInput slice = wrapped.slice("slice", 0, len);
            for (int i = 0; i < values; i++) {
                assertEquals(input.readShort(), EndiannessReverserUtil.readShort(slice));
            }
            slice.close();
            input.close();
            wrapped.close();
        }
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            RandomAccessInput randomAccessInput = input.randomAccessSlice(0, len);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            RandomAccessInput randomAccessWrapped = wrapped.randomAccessSlice(0, len);
            for (int i = 0; i < values; i++) {
                assertEquals(randomAccessInput.readShort(2 * i), EndiannessReverserUtil.readShort(randomAccessWrapped, 2 * i));
            }
            input.close();
            wrapped.close();
        }
        directory.close();
    }

    public void testReadInt() throws IOException {
        Directory directory = newDirectory();
        IndexOutput output = directory.createOutput("endianness", IOContext.DEFAULT);
        int values = atLeast(30);
        boolean useHelper = random().nextBoolean();
        for (int i = 0; i < values; i++) {
            if (useHelper) {
                EndiannessReverserUtil.writeInt(output, random().nextInt());
            } else {
                output.writeInt(random().nextInt());
            }
        }
        long len = output.getFilePointer();
        output.close();
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            for (int i = 0; i < values; i++) {
                assertEquals(input.readInt(), EndiannessReverserUtil.readInt(wrapped));
            }
            input.close();
            wrapped.close();
        }
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            IndexInput slice = wrapped.slice("slice", 0, len);
            for (int i = 0; i < values; i++) {
                assertEquals(input.readInt(), EndiannessReverserUtil.readInt(slice));
            }
            slice.close();
            input.close();
            wrapped.close();
        }
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            RandomAccessInput randomAccessInput = input.randomAccessSlice(0, len);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            RandomAccessInput randomAccessWrapped = wrapped.randomAccessSlice(0, len);
            for (int i = 0; i < values; i++) {
                assertEquals(randomAccessInput.readInt(4 * i), EndiannessReverserUtil.readInt(randomAccessWrapped, 4 * i));
            }
            input.close();
            wrapped.close();
        }
        directory.close();
    }

    public void testReadLong() throws IOException {
        Directory directory = newDirectory();
        IndexOutput output = directory.createOutput("endianness", IOContext.DEFAULT);
        int values = atLeast(30);
        boolean useHelper = random().nextBoolean();
        for (int i = 0; i < values; i++) {
            if (useHelper) {
                EndiannessReverserUtil.writeLong(output, random().nextLong());
            } else {
                output.writeLong(random().nextLong());
            }
        }
        long len = output.getFilePointer();
        output.close();
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            for (int i = 0; i < values; i++) {
                assertEquals(input.readLong(), EndiannessReverserUtil.readLong(wrapped));
            }
            input.close();
            wrapped.close();
        }
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            IndexInput slice = wrapped.slice("slice", 0, len);
            for (int i = 0; i < values; i++) {
                assertEquals(input.readLong(), EndiannessReverserUtil.readLong(slice));
            }
            slice.close();
            input.close();
            wrapped.close();
        }
        {
            IndexInput input = directory.openInput("endianness", IOContext.DEFAULT);
            RandomAccessInput randomAccessInput = input.randomAccessSlice(0, len);
            EndiannessReverserIndexInput wrapped = new EndiannessReverserIndexInput(directory.openInput("endianness", IOContext.DEFAULT));
            RandomAccessInput randomAccessWrapped = wrapped.randomAccessSlice(0, len);
            for (int i = 0; i < values; i++) {
                assertEquals(randomAccessInput.readLong(8 * i), EndiannessReverserUtil.readLong(randomAccessWrapped, 8 * i));
            }
            input.close();
            wrapped.close();
        }
        directory.close();
    }
}
