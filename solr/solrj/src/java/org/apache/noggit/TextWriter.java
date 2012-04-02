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

package org.apache.noggit;

import java.util.*;

/**
 * @author yonik
 * @version $Id$
 */
public abstract class TextWriter {
  public abstract void writeNull();

  public abstract void writeString(CharSequence str);

  public abstract void writeString(CharArr str);

  public abstract void writeStringStart();
  public abstract void writeStringChars(CharArr partialStr);
  public abstract void writeStringEnd();

  public abstract void write(long number);

  public abstract void write(double number);

  public abstract void write(boolean bool);

  public abstract void writeNumber(CharArr digits);
  
  public abstract void writePartialNumber(CharArr digits);

  public abstract void startObject();

  public abstract void endObject();

  public abstract void startArray();

  public abstract void endArray();

  public abstract void writeValueSeparator();

  public abstract void writeNameSeparator();

  // void writeNameValue(String name, Object val)?
}

