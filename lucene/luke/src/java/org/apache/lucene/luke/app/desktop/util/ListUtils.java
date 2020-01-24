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

package org.apache.lucene.luke.app.desktop.util;

import javax.swing.JList;
import javax.swing.ListModel;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** List model utilities */
public class ListUtils {

  public static <T> List<T> getAllItems(JList<T> jlist) {
    ListModel<T> model = jlist.getModel();
    return getAllItems(jlist, model::getElementAt);
  }

  public static <T, R> List<R> getAllItems(JList<T> jlist, IntFunction<R> mapFunc) {
    ListModel<T> model = jlist.getModel();
    return IntStream.range(0, model.getSize()).mapToObj(mapFunc).collect(Collectors.toList());
  }

  private ListUtils() {
  }

}
