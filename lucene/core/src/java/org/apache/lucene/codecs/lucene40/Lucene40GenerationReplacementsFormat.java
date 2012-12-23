package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.lucene.codecs.GenerationReplacementsFormat;
import org.apache.lucene.index.FieldGenerationReplacements;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

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

public class Lucene40GenerationReplacementsFormat extends
    GenerationReplacementsFormat {
  
  @Override
  protected FieldGenerationReplacements readPersistedGeneration(IndexInput input)
      throws IOException {
    final int size = input.readVInt();
    FieldGenerationReplacements reps = new FieldGenerationReplacements();
    int curr = 0;
    for (int i = 0; i < size; i++) {
      curr += input.readVInt();
      reps.set(curr, input.readVLong());
    }
    return reps;
  }
  
  @Override
  protected void persistGeneration(FieldGenerationReplacements reps,
      IndexOutput output) throws IOException {
    // write number of replacements
    output.writeVInt(reps.size());
    
    // write replacements
    int prev = 0;
    for (Entry<Integer,Long> entry : reps){
      final int curr = entry.getKey();
      output.writeVInt(curr - prev);
      prev = curr;
      output.writeVLong(entry.getValue());
    }
  }
  
}
