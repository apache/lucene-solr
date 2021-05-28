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
package org.apache.lucene.analysis.hunspell;

enum WordContext {
  /** non-compound */
  SIMPLE_WORD,

  /** The first root in a word with COMPOUNDFLAG/BEGIN/MIDDLE/END compounding */
  COMPOUND_BEGIN,

  /** A middle root in a word with COMPOUNDFLAG/BEGIN/MIDDLE/END compounding */
  COMPOUND_MIDDLE,

  /** The final root in a word with COMPOUNDFLAG/BEGIN/MIDDLE/END compounding */
  COMPOUND_END,

  /**
   * The final root in a word with COMPOUNDRULE compounding. The difference to {@link #COMPOUND_END}
   * is that this context doesn't require COMPOUNDFLAG/COMPOUNDEND flags, but allows ONLYINCOMPOUND.
   */
  COMPOUND_RULE_END;

  boolean isCompound() {
    return this != SIMPLE_WORD;
  }

  boolean isAffixAllowedWithoutSpecialPermit(boolean isPrefix) {
    if (isPrefix) {
      return this == WordContext.COMPOUND_BEGIN;
    }
    return this == WordContext.COMPOUND_END || this == WordContext.COMPOUND_RULE_END;
  }

  char requiredFlag(Dictionary dictionary) {
    switch (this) {
      case COMPOUND_BEGIN:
        return dictionary.compoundBegin;
      case COMPOUND_MIDDLE:
        return dictionary.compoundMiddle;
      case COMPOUND_END:
        return dictionary.compoundEnd;
      case COMPOUND_RULE_END:
      case SIMPLE_WORD:
      default:
        return Dictionary.FLAG_UNSET;
    }
  }
}
