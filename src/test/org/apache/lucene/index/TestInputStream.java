package org.apache.lucene.index;

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

import junit.framework.TestCase;
import org.apache.lucene.store.InputStream;

import java.io.IOException;

public class TestInputStream extends TestCase {
    public void testRead() throws IOException {
        InputStream is = new MockInputStream(new byte[] { (byte) 0x80, 0x01,
                                                          (byte) 0xFF, 0x7F,
                                                          (byte) 0x80, (byte) 0x80, 0x01,
                                                          (byte) 0x81, (byte) 0x80, 0x01,
                                                          0x06, 'L', 'u', 'c', 'e', 'n', 'e'});
        assertEquals(128,is.readVInt());
        assertEquals(16383,is.readVInt());
        assertEquals(16384,is.readVInt());
        assertEquals(16385,is.readVInt());
        assertEquals("Lucene",is.readString());
    }
}
