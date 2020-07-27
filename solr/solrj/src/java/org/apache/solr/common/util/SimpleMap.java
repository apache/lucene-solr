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

package org.apache.solr.common.util;

import org.apache.solr.common.MapWriter;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A simplified read-only key-value structure. It is designed to support large datasets without consuming lot of memory
 * The objective is to provide implementations that are cheap and memory efficient to implement and consume.
 * The keys are always {@link CharSequence} objects, The values can be of any type
 */
public interface SimpleMap<T> extends MapWriter {

  /**get a value by key. If not present , null is returned */
  T get(CharSequence key);

  /**Navigate through all keys and values */
  void forEach(BiConsumer<? super CharSequence, ? super T> fun);

  /** iterate through all keys
   * The default impl is suboptimal. Proper implementations must do it more efficiently
   * */
  default void forEachKey(Consumer<CharSequence> fun) {
    forEach((k, t) -> fun.accept(k));
  }

  /**
   * iterate through all keys but stop in between if required
   *  The default impl is suboptimal. Proper implementations must do it more efficiently
   * @param fun Consume each key and return a boolean to signal whether to proceed or not. If true , continue. If false stop
   * */
  default void conditionalForEachKey(Function<? super CharSequence, Boolean> fun){
    forEachKey(new Consumer<>() {
      boolean end = false;
      @Override
      public void accept(CharSequence k) {
        if (end) return;
        end = fun.apply(k);
      }
    });
  }


  /**
   * Navigate through all keys but stop in between if required.
   * The default impl is suboptimal. Proper implementations must do it more efficiently
   * @param fun Consume each entry and return a boolean to signal whether to proceed or not. If true, continue, if false stop
   */
  default void conditionalForEach(BiFunction<? super CharSequence, ? super T, Boolean> fun) {
    forEach(new BiConsumer<>() {
      boolean end = false;
      @Override
      public void accept(CharSequence k, T v) {
        if (end) return;
        end = fun.apply(k, v);
      }
    });
  }

  /**
   * Navigate through all keys from a starting point but stop in between if required.
   * The default impl is suboptimal. Proper implementations must do it more efficiently
   * @param fun Consume each entry and return a boolean to signal whether to proceed or not. If true, continue, if false stop
   */
  default void conditionalForEach(CharSequence start, BiFunction<? super CharSequence, ? super T, Boolean> fun) {
    conditionalForEach(new BiFunction<>() {
      boolean end = false;
      @Override
      public Boolean apply(CharSequence k, T v) {
        if (end) return Boolean.FALSE;
        if (k == null || start == null) {
          end = fun.apply(k, v);
        } else if (k.toString().compareTo(start.toString()) >= 0) {
          end = fun.apply(k, v);
        }
        return Boolean.TRUE;
      }
    });

  }


  @Override
  default void writeMap(EntryWriter ew) throws IOException {
    forEach(ew::putNoEx);
  }
}
