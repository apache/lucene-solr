package org.apache.lucene.codecs.simpletext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.lucene.codecs.GenerationReplacementsFormat;
import org.apache.lucene.index.FieldGenerationReplacements;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

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

public class SimpleTextGenerationReplacementsFormat extends
    GenerationReplacementsFormat {
  final static BytesRef FGR_DOCCOUNT   = new BytesRef("    number of documents ");
  final static BytesRef FGR_DOC        = new BytesRef("    doc ");
  final static BytesRef FGR_GENERATION = new BytesRef("      generation ");

  @Override
  protected FieldGenerationReplacements readPersistedGeneration(IndexInput input)
      throws IOException {
    FieldGenerationReplacements reps = new FieldGenerationReplacements();
    
    BytesRef scratch = new BytesRef();
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, FGR_DOCCOUNT);
    final int size = Integer.parseInt(readString(FGR_DOCCOUNT.length, scratch));
    
    for (int i = 0; i < size; i++) {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, FGR_DOC);
      final int doc = Integer.parseInt(readString(FGR_DOC.length, scratch));
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, FGR_GENERATION);
      final long generation = Integer.parseInt(readString(FGR_GENERATION.length, scratch));
      
      reps.set(doc, generation);
    }

    return reps;
  }
  
  private String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset+offset, scratch.length-offset, IOUtils.CHARSET_UTF_8);
  }

  @Override
  protected void persistGeneration(FieldGenerationReplacements reps,
      IndexOutput output) throws IOException {
    BytesRef scratch = new BytesRef();
    SimpleTextUtil.write(output, FGR_DOCCOUNT);
    SimpleTextUtil.write(output, Integer.toString(reps.size()), scratch);
    SimpleTextUtil.writeNewline(output);

    for (Entry<Integer,Long> entry : reps){
      SimpleTextUtil.write(output, FGR_DOC);
      SimpleTextUtil.write(output, Integer.toString(entry.getKey()), scratch);
      SimpleTextUtil.writeNewline(output);
      SimpleTextUtil.write(output, FGR_GENERATION);
      SimpleTextUtil.write(output, Long.toString(entry.getValue()), scratch);
      SimpleTextUtil.writeNewline(output);
    }
  }
  
}
