package org.apache.lucene.demo.html;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public final class Tags {

  /**
   * contains all tags for which whitespaces have to be inserted for proper tokenization
   */
  public static final Set<String> WS_ELEMS = Collections.synchronizedSet(new HashSet<String>());

  static{
    WS_ELEMS.add("<hr");
    WS_ELEMS.add("<hr/");  // note that "<hr />" does not need to be listed explicitly
    WS_ELEMS.add("<br");
    WS_ELEMS.add("<br/");
    WS_ELEMS.add("<p");
    WS_ELEMS.add("</p");
    WS_ELEMS.add("<div");
    WS_ELEMS.add("</div");
    WS_ELEMS.add("<td");
    WS_ELEMS.add("</td");
    WS_ELEMS.add("<li");
    WS_ELEMS.add("</li");
    WS_ELEMS.add("<q");
    WS_ELEMS.add("</q");
    WS_ELEMS.add("<blockquote");
    WS_ELEMS.add("</blockquote");
    WS_ELEMS.add("<dt");
    WS_ELEMS.add("</dt");
    WS_ELEMS.add("<h1");
    WS_ELEMS.add("</h1");
    WS_ELEMS.add("<h2");
    WS_ELEMS.add("</h2");
    WS_ELEMS.add("<h3");
    WS_ELEMS.add("</h3");
    WS_ELEMS.add("<h4");
    WS_ELEMS.add("</h4");
    WS_ELEMS.add("<h5");
    WS_ELEMS.add("</h5");
    WS_ELEMS.add("<h6");
    WS_ELEMS.add("</h6");
  }
}
