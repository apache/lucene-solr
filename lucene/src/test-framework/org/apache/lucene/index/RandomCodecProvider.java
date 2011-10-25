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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.memory.MemoryCodec;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;
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
public class RandomCodecProvider extends CodecProvider {
  private List<Codec> knownCodecs = new ArrayList<Codec>();
  private Map<String,Codec> previousMappings = new HashMap<String,Codec>();
  private final int perFieldSeed;
  
  public RandomCodecProvider(Random random, boolean useNoMemoryExpensiveCodec) {
    this.perFieldSeed = random.nextInt();
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    int minItemsPerBlock = _TestUtil.nextInt(random, 2, 100);
    int maxItemsPerBlock = 2*(Math.max(2, minItemsPerBlock-1)) + random.nextInt(100);
    register(new StandardCodec(minItemsPerBlock, maxItemsPerBlock));
    register(new PreFlexCodec());
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    minItemsPerBlock = _TestUtil.nextInt(random, 2, 100);
    maxItemsPerBlock = 2*(Math.max(1, minItemsPerBlock-1)) + random.nextInt(100);
    register(new PulsingCodec( 1 + random.nextInt(20), minItemsPerBlock, maxItemsPerBlock));
    if (!useNoMemoryExpensiveCodec) {
      register(new SimpleTextCodec());
      register(new MemoryCodec());
    }
    Collections.shuffle(knownCodecs, random);
  }
  
  @Override
  public synchronized void register(Codec codec) {
    if (!codec.name.equals("PreFlex"))
      knownCodecs.add(codec);
    super.register(codec);
  }
  
  @Override
  public synchronized void unregister(Codec codec) {
    knownCodecs.remove(codec);
    super.unregister(codec);
  }
  
  @Override
  public synchronized String getFieldCodec(String name) {
    Codec codec = previousMappings.get(name);
    if (codec == null) {
      codec = knownCodecs.get(Math.abs(perFieldSeed ^ name.hashCode()) % knownCodecs.size());
      if (codec instanceof SimpleTextCodec && perFieldSeed % 5 != 0) {
        // make simpletext rarer, choose again
        codec = knownCodecs.get(Math.abs(perFieldSeed ^ name.toUpperCase(Locale.ENGLISH).hashCode()) % knownCodecs.size());
      }
      previousMappings.put(name, codec);
    }
    return codec.name;
  }
  
  @Override
  public synchronized boolean hasFieldCodec(String name) {
    return true; // we have a codec for every field
  }
  
  @Override
  public synchronized String toString() {
    return "RandomCodecProvider: " + previousMappings.toString();
  }
}
