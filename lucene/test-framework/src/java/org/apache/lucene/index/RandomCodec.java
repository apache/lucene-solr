package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.codecs.lucene40ords.Lucene40WithOrds;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.mockintblock.MockFixedIntBlockPostingsFormat;
import org.apache.lucene.codecs.mockintblock.MockVariableIntBlockPostingsFormat;
import org.apache.lucene.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.codecs.mocksep.MockSepPostingsFormat;
import org.apache.lucene.codecs.nestedpulsing.NestedPulsingPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextPostingsFormat;
import org.apache.lucene.util._TestUtil;

/**
 * Codec that assigns per-field random postings formats.
 * <p>
 * The same field/format assignment will happen regardless of order,
 * a hash is computed up front that determines the mapping.
 * This means fields can be put into things like HashSets and added to
 * documents in different orders and the test will still be deterministic
 * and reproducable.
 */
public class RandomCodec extends Lucene40Codec {
  /** shuffled list of postingsformats to use for new mappings */
  private List<PostingsFormat> formats = new ArrayList<PostingsFormat>();
  /** memorized field->postingsformat mappings */
  // note: we have to sync this map even though its just for debugging/toString, 
  // otherwise DWPT's .toString() calls that iterate over the map can 
  // cause concurrentmodificationexception if indexwriter's infostream is on
  private Map<String,PostingsFormat> previousMappings = Collections.synchronizedMap(new HashMap<String,PostingsFormat>());
  private final int perFieldSeed;

  @Override
  public PostingsFormat getPostingsFormatForField(String name) {
    PostingsFormat codec = previousMappings.get(name);
    if (codec == null) {
      codec = formats.get(Math.abs(perFieldSeed ^ name.hashCode()) % formats.size());
      if (codec instanceof SimpleTextPostingsFormat && perFieldSeed % 5 != 0) {
        // make simpletext rarer, choose again
        codec = formats.get(Math.abs(perFieldSeed ^ name.toUpperCase(Locale.ENGLISH).hashCode()) % formats.size());
      }
      previousMappings.put(name, codec);
      // Safety:
      assert previousMappings.size() < 10000;
    }
    return codec;
  }

  public RandomCodec(Random random, boolean useNoMemoryExpensiveCodec) {
    this.perFieldSeed = random.nextInt();
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    int minItemsPerBlock = _TestUtil.nextInt(random, 2, 100);
    int maxItemsPerBlock = 2*(Math.max(2, minItemsPerBlock-1)) + random.nextInt(100);
    formats.add(new Lucene40PostingsFormat(minItemsPerBlock, maxItemsPerBlock));
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    minItemsPerBlock = _TestUtil.nextInt(random, 2, 100);
    maxItemsPerBlock = 2*(Math.max(1, minItemsPerBlock-1)) + random.nextInt(100);
    formats.add(new Pulsing40PostingsFormat(1 + random.nextInt(20), minItemsPerBlock, maxItemsPerBlock));
    formats.add(new MockSepPostingsFormat());
    formats.add(new MockFixedIntBlockPostingsFormat(_TestUtil.nextInt(random, 1, 2000)));
    formats.add(new MockVariableIntBlockPostingsFormat( _TestUtil.nextInt(random, 1, 127)));
    formats.add(new MockRandomPostingsFormat(random));
    formats.add(new NestedPulsingPostingsFormat());
    formats.add(new Lucene40WithOrds());
    if (!useNoMemoryExpensiveCodec) {
      formats.add(new SimpleTextPostingsFormat());
      formats.add(new MemoryPostingsFormat(random.nextBoolean()));
    }
    Collections.shuffle(formats, random);
  }
  
  @Override
  public String toString() {
    return super.toString() + ": " + previousMappings.toString();
  }
}
