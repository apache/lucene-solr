package org.apache.lucene.facet.index;

import java.io.ByteArrayInputStream;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.CategoryListPayloadStream;
import org.apache.lucene.util.encoding.DGapIntDecoder;
import org.apache.lucene.util.encoding.DGapIntEncoder;
import org.apache.lucene.util.encoding.IntDecoder;
import org.apache.lucene.util.encoding.NOnesIntDecoder;
import org.apache.lucene.util.encoding.NOnesIntEncoder;
import org.apache.lucene.util.encoding.UniqueValuesIntEncoder;

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

public class CategoryListPayloadStreamTest extends LuceneTestCase {

  /**
   * Verifies that a CategoryListPayloadStream can properly encode values into
   * a byte stream for later constructing a payload.
   */
  @Test
  public void testStream() throws Exception {

    CategoryListPayloadStream clps = new CategoryListPayloadStream(
        new UniqueValuesIntEncoder(new DGapIntEncoder(
            new NOnesIntEncoder(3))));

    clps.appendIntToStream(1);
    clps.appendIntToStream(10);
    clps.appendIntToStream(100);
    clps.appendIntToStream(1000);
    clps.appendIntToStream(10000);
    clps.appendIntToStream(100000);
    clps.appendIntToStream(1000000);
    clps.appendIntToStream(10000000);
    clps.appendIntToStream(100000000);
    clps.appendIntToStream(1000000000);
    clps.appendIntToStream(Integer.MAX_VALUE);

    ByteArrayInputStream bais = new ByteArrayInputStream(clps
        .convertStreamToByteArray());
    IntDecoder decoder = new DGapIntDecoder(new NOnesIntDecoder(3));
    decoder.reInit(bais);
    assertEquals("Wrong value in byte stream", 1, decoder.decode());
    assertEquals("Wrong value in byte stream", 10, decoder.decode());
    assertEquals("Wrong value in byte stream", 100, decoder.decode());
    assertEquals("Wrong value in byte stream", 1000, decoder.decode());
    assertEquals("Wrong value in byte stream", 10000, decoder.decode());
    assertEquals("Wrong value in byte stream", 100000, decoder.decode());
    assertEquals("Wrong value in byte stream", 1000000, decoder.decode());
    assertEquals("Wrong value in byte stream", 10000000, decoder.decode());
    assertEquals("Wrong value in byte stream", 100000000, decoder.decode());
    assertEquals("Wrong value in byte stream", 1000000000, decoder.decode());
    assertEquals("Wrong value in byte stream", Integer.MAX_VALUE, decoder.decode());
    assertEquals("End of stream not reached", IntDecoder.EOS, decoder.decode());

    clps.reset();
    decoder.reInit(bais);
    assertEquals("End of stream not reached", IntDecoder.EOS, decoder.decode());
  }

}
