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
package org.apache.solr.search.join;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.search.SyntaxError;

class ScoreModeParser {
  final private static Map<String, ScoreMode> lowerAndCapitalCase = 
      Collections.unmodifiableMap( new HashMap<String, ScoreMode>() {
    {
      for (ScoreMode s : ScoreMode.values()) {
        put(s.name().toLowerCase(Locale.ROOT), s);
        put(s.name(), s);
      }
    }
  });

  private ScoreModeParser(){}
  
  /** 
   * recognizes as-is {@link ScoreMode} names, and lowercase as well, 
   * otherwise throws exception 
   * @throws SyntaxError when it's unable to parse
   * */
  static ScoreMode parse(String score) throws SyntaxError {
    final ScoreMode scoreMode = lowerAndCapitalCase.get(score);
    if (scoreMode == null) {
      throw new SyntaxError("Unable to parse ScoreMode from: " + score);
    }
    return scoreMode;
  }
  
}
