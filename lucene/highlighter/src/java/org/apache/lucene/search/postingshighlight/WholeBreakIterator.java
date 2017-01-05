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
package org.apache.lucene.search.postingshighlight;

import java.text.BreakIterator;
import java.text.CharacterIterator;

/** Just produces one single fragment for the entire text */
public final class WholeBreakIterator extends BreakIterator {
  private CharacterIterator text;
  private int start;
  private int end;
  private int current;

  @Override
  public int current() {
    return current;
  }

  @Override
  public int first() {
    return (current = start);
  }

  @Override
  public int following(int pos) {
    if (pos < start || pos > end) {
      throw new IllegalArgumentException("offset out of bounds");
    } else if (pos == end) {
      // this conflicts with the javadocs, but matches actual behavior (Oracle has a bug in something)
      // https://bugs.openjdk.java.net/browse/JDK-8015110
      current = end;
      return DONE;
    } else {
      return last();
    }
  }

  @Override
  public CharacterIterator getText() {
    return text;
  }

  @Override
  public int last() {
    return (current = end);
  }

  @Override
  public int next() {
    if (current == end) {
      return DONE;
    } else {
      return last();
    }
  }

  @Override
  public int next(int n) {
    if (n < 0) {
      for (int i = 0; i < -n; i++) {
        previous();
      }
    } else {
      for (int i = 0; i < n; i++) {
        next();
      }
    }
    return current();
  }

  @Override
  public int preceding(int pos) {
    if (pos < start || pos > end) {
      throw new IllegalArgumentException("offset out of bounds");
    } else if (pos == start) {
      // this conflicts with the javadocs, but matches actual behavior (Oracle has a bug in something)
      // https://bugs.openjdk.java.net/browse/JDK-8015110
      current = start;
      return DONE;
    } else {
      return first();
    }
  }

  @Override
  public int previous() {
    if (current == start) {
      return DONE;
    } else {
      return first();
    }
  }

  @Override
  public void setText(CharacterIterator newText) {
    start = newText.getBeginIndex();
    end = newText.getEndIndex();
    text = newText;
    current = start;
  }
}
