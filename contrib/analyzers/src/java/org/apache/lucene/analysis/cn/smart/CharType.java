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

package org.apache.lucene.analysis.cn.smart;

public class CharType {

  public final static int DELIMITER = 0;

  public final static int LETTER = 1;

  public final static int DIGIT = 2;

  public final static int HANZI = 3;

  public final static int SPACE_LIKE = 4;

  // (全角半角)标点符号，半角（字母，数字），汉字，空格，"\t\r\n"等空格或换行字符
  public final static int FULLWIDTH_LETTER = 5;

  public final static int FULLWIDTH_DIGIT = 6; // 全角字符，字母，数字

  public final static int OTHER = 7;

}
