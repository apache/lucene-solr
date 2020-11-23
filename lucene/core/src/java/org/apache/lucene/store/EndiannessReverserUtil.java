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

import java.io.IOException;

/**
 * Helper methods to revert the endianness of the Input / Output methods.
 * 
 * @lucene.internal
 */
public class EndiannessReverserUtil {
    
    private EndiannessReverserUtil() {
        // no instances
    }
    
    /** read a short from the provided DataInput and reverse endianness */
    public static short readShort(DataInput in) throws IOException {
        return Short.reverseBytes(in.readShort());
    }

    /** read an integer from the provided DataInput and reverse endianness */
    public static int readInt(DataInput in) throws IOException {
        return Integer.reverseBytes(in.readInt());
    }

    /** read a long from the provided DataInput and reverse endianness */
    public static long readLong(DataInput in) throws IOException {
        return Long.reverseBytes(in.readLong());
    }

    /** write a short to the provided DataOutput with reverse endianness */
    public static void writeShort(DataOutput out, short i) throws IOException {
        out.writeShort(Short.reverseBytes(i));
    }

    /** write an integer to the provided DataOutput with reverse endianness */
    public static void writeInt(DataOutput out, int i) throws IOException {
        out.writeInt(Integer.reverseBytes(i));
    }

    /** write a long to the provided DataOutput with reverse endianness */
    public static void writeLong(DataOutput out, long i) throws IOException {
        out.writeLong(Long.reverseBytes(i));
    }

    /** read a short from the provided RandomAccessInput and reverse endianness */
    public static short readShort(RandomAccessInput in, long pos) throws IOException {
        return Short.reverseBytes(in.readShort(pos));
    }

    /** read an integer from the provided RandomAccessInput and reverse endianness */
    public static int readInt(RandomAccessInput in, long pos) throws IOException {
        return Integer.reverseBytes(in.readInt(pos));
    }

    /** read a long from the provided RandomAccessInput and reverse endianness */
    public static long readLong(RandomAccessInput in, long pos) throws IOException {
        return Long.reverseBytes(in.readLong(pos));
    }
}
