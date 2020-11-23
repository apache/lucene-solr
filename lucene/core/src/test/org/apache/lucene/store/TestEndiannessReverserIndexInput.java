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
