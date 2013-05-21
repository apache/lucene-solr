package org.apache.lucene.analysis.stages;

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

import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.Version;

public class LowerCaseFilterStage extends Stage {
  private final CharacterUtils charUtils;
  private final CharTermAttribute termAttOut;
  private final CharTermAttribute termAttIn;

  public LowerCaseFilterStage(Version version, Stage prevStage) {
    super(prevStage);
    charUtils = CharacterUtils.getInstance(version);
    termAttIn = prevStage.get(CharTermAttribute.class);
    termAttOut = create(CharTermAttribute.class);
  }
  
  @Override
  public final boolean next() throws IOException {
    if (prevStage.next()) {
      final char[] buffer = termAttIn.buffer();
      final int length = termAttIn.length();
      final char[] bufferOut = termAttOut.resizeBuffer(length);
      for (int i = 0; i < length;) {
        i += Character.toChars(
                Character.toLowerCase(
                   charUtils.codePointAt(buffer, i)), bufferOut, i);
      }
      termAttOut.setLength(length);
      return true;
    } else {
      return false;
    }
  }
}
