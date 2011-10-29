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

import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsBaseFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.index.codecs.mockintblock.MockFixedIntBlockPostingsFormat;
import org.apache.lucene.index.codecs.mockintblock.MockVariableIntBlockPostingsFormat;
import org.apache.lucene.index.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.index.codecs.mocksep.MockSepPostingsFormat;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsFormat;
import org.apache.lucene.index.codecs.simpletext.SimpleTextPostingsFormat;
import org.apache.lucene.util._TestUtil;

/**
 * CodecProvider that assigns per-field random codecs.
 * <p>
 * The same field/codec assignment will happen regardless of order,
 * a hash is computed up front that determines the mapping.
 * This means fields can be put into things like HashSets and added to
 * documents in different orders and the test will still be deterministic
 * and reproducable.
 */
public class RandomCodec extends Lucene40Codec {
  /** name->postingsformat mappings */
  private Map<String,PostingsFormat> formatNames = new HashMap<String,PostingsFormat>();
  /** shuffled list of postingsformats to use for new mappings */
  private List<PostingsFormat> knownFormats = new ArrayList<PostingsFormat>();
  /** memorized field->postingsformat mappings */
  private Map<String,PostingsFormat> previousMappings = new HashMap<String,PostingsFormat>();
  private final int perFieldSeed;
  private final String configuration;
  
  @Override
  public PostingsFormat getPostingsFormat(String formatName) {
    return super.getPostingsFormat(formatName);
  }

  @Override
  public synchronized String getPostingsFormatForField(String name) {
    if ("random".equals(configuration)) {
      PostingsFormat codec = previousMappings.get(name);
      if (codec == null) {
        codec = knownFormats.get(Math.abs(perFieldSeed ^ name.hashCode()) % knownFormats.size());
        if (codec instanceof SimpleTextPostingsFormat && perFieldSeed % 5 != 0) {
          // make simpletext rarer, choose again
          codec = knownFormats.get(Math.abs(perFieldSeed ^ name.toUpperCase(Locale.ENGLISH).hashCode()) % knownFormats.size());
        }
        previousMappings.put(name, codec);
      }
      return codec.name;
    } else {
      return configuration;
    }
  }

  public RandomCodec(Random random, boolean useNoMemoryExpensiveCodec, String configuration) {
    this.configuration = configuration;
    this.perFieldSeed = random.nextInt();
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    int minItemsPerBlock = _TestUtil.nextInt(random, 2, 100);
    int maxItemsPerBlock = 2*(Math.max(2, minItemsPerBlock-1)) + random.nextInt(100);
    register(new Lucene40PostingsFormat(minItemsPerBlock, maxItemsPerBlock));
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    minItemsPerBlock = _TestUtil.nextInt(random, 2, 100);
    maxItemsPerBlock = 2*(Math.max(1, minItemsPerBlock-1)) + random.nextInt(100);
    register(new PulsingPostingsFormat(new Lucene40PostingsBaseFormat(), 1 + random.nextInt(20), minItemsPerBlock, maxItemsPerBlock));
    register(new MockSepPostingsFormat());
    register(new MockFixedIntBlockPostingsFormat(_TestUtil.nextInt(random, 1, 2000)));
    register(new MockVariableIntBlockPostingsFormat( _TestUtil.nextInt(random, 1, 127)));
    register(new MockRandomPostingsFormat(random));
    if (!useNoMemoryExpensiveCodec) {
      register(new SimpleTextPostingsFormat());
      register(new MemoryPostingsFormat());
    }
    Collections.shuffle(knownFormats, random);
  }
  
  public synchronized void register(PostingsFormat format) {
    formatNames.put(format.name, format);
    knownFormats.add(format);
  }
  
  // TODO: needed anymore? I don't think so
  public synchronized void unregister(PostingsFormat format) {
    knownFormats.remove(format);
  }
  
  @Override
  public synchronized String toString() {
    return "RandomCodec: " + previousMappings.toString();
  }
}
