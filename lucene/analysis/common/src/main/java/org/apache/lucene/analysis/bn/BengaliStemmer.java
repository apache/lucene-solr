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
package org.apache.lucene.analysis.bn;


import static org.apache.lucene.analysis.util.StemmerUtil.endsWith;

/**
 * Stemmer for Bengali.
 * <p>
 * The algorithm is based on the report in:
 * <i>Natural Language Processing in an Indian Language (Bengali)-I: Verb Phrase Analysis</i>
 * P Sengupta and B B Chaudhuri
 * </p>
 *
 * <p>
 *   Few Stemmer criteria are taken from:
 *   <i>http://members.unine.ch/jacques.savoy/clef/BengaliStemmerLight.java.txt</i>
 * </p>
 */
public class BengaliStemmer {
  public int stem(char buffer[], int len) {

    // 8
    if (len > 9 && (endsWith(buffer, len, "িয়াছিলাম")
        || endsWith(buffer, len, "িতেছিলাম")
        || endsWith(buffer, len, "িতেছিলেন")
        || endsWith(buffer, len, "ইতেছিলেন")
        || endsWith(buffer, len, "িয়াছিলেন")
        || endsWith(buffer, len, "ইয়াছিলেন")
    ))
      return len - 8;

    // 7
    if ((len > 8) && (endsWith(buffer, len, "িতেছিলি")
        || endsWith(buffer, len, "িতেছিলে")
        || endsWith(buffer, len, "িয়াছিলা")
        || endsWith(buffer, len, "িয়াছিলে")
        || endsWith(buffer, len, "িতেছিলা")
        || endsWith(buffer, len, "িয়াছিলি")

        || endsWith(buffer, len, "য়েদেরকে")
    ))
      return len - 7;

    // 6
    if ((len > 7) && (endsWith(buffer, len, "িতেছিস")
        || endsWith(buffer, len, "িতেছেন")
        || endsWith(buffer, len, "িয়াছিস")
        || endsWith(buffer, len, "িয়াছেন")
        || endsWith(buffer, len, "েছিলাম")
        || endsWith(buffer, len, "েছিলেন")

        || endsWith(buffer, len, "েদেরকে")
    ))
      return len - 6;

    // 5
    if ((len > 6) && (endsWith(buffer, len, "িতেছি")
        || endsWith(buffer, len, "িতেছা")
        || endsWith(buffer, len, "িতেছে")
        || endsWith(buffer, len, "ছিলাম")
        || endsWith(buffer, len, "ছিলেন")
        || endsWith(buffer, len, "িয়াছি")
        || endsWith(buffer, len, "িয়াছা")
        || endsWith(buffer, len, "িয়াছে")
        || endsWith(buffer, len, "েছিলে")
        || endsWith(buffer, len, "েছিলা")

        || endsWith(buffer, len, "য়েদের")
        || endsWith(buffer, len, "দেরকে")
    ))
      return len - 5;

    // 4
    if ((len > 5) && (endsWith(buffer, len, "িলাম")
        || endsWith(buffer, len, "িলেন")
        || endsWith(buffer, len, "িতাম")
        || endsWith(buffer, len, "িতেন")
        || endsWith(buffer, len, "িবেন")
        || endsWith(buffer, len, "ছিলি")
        || endsWith(buffer, len, "ছিলে")
        || endsWith(buffer, len, "ছিলা")
        || endsWith(buffer, len, "তেছে")
        || endsWith(buffer, len, "িতেছ")

        || endsWith(buffer, len, "খানা")
        || endsWith(buffer, len, "খানি")
        || endsWith(buffer, len, "গুলো")
        || endsWith(buffer, len, "গুলি")
        || endsWith(buffer, len, "য়েরা")
        || endsWith(buffer, len, "েদের")
    ))
      return len - 4;

    // 3
    if ((len > 4) && (endsWith(buffer, len, "লাম")
        || endsWith(buffer, len, "িলি")
        || endsWith(buffer, len, "ইলি")
        || endsWith(buffer, len, "িলে")
        || endsWith(buffer, len, "ইলে")
        || endsWith(buffer, len, "লেন")
        || endsWith(buffer, len, "িলা")
        || endsWith(buffer, len, "ইলা")
        || endsWith(buffer, len, "তাম")
        || endsWith(buffer, len, "িতি")
        || endsWith(buffer, len, "ইতি")
        || endsWith(buffer, len, "িতে")
        || endsWith(buffer, len, "ইতে")
        || endsWith(buffer, len, "তেন")
        || endsWith(buffer, len, "িতা")
        || endsWith(buffer, len, "িবা")
        || endsWith(buffer, len, "ইবা")
        || endsWith(buffer, len, "িবি")
        || endsWith(buffer, len, "ইবি")
        || endsWith(buffer, len, "বেন")
        || endsWith(buffer, len, "িবে")
        || endsWith(buffer, len, "ইবে")
        || endsWith(buffer, len, "ছেন")

        || endsWith(buffer, len, "য়োন")
        || endsWith(buffer, len, "য়ের")
        || endsWith(buffer, len, "েরা")
        || endsWith(buffer, len, "দের")
    ))
      return len - 3;

    // 2
    if ((len > 3) && (endsWith(buffer, len, "িস")
        || endsWith(buffer, len, "েন")
        || endsWith(buffer, len, "লি")
        || endsWith(buffer, len, "লে")
        || endsWith(buffer, len, "লা")
        || endsWith(buffer, len, "তি")
        || endsWith(buffer, len, "তে")
        || endsWith(buffer, len, "তা")
        || endsWith(buffer, len, "বি")
        || endsWith(buffer, len, "বে")
        || endsWith(buffer, len, "বা")
        || endsWith(buffer, len, "ছি")
        || endsWith(buffer, len, "ছা")
        || endsWith(buffer, len, "ছে")
        || endsWith(buffer, len, "ুন")
        || endsWith(buffer, len, "ুক")

        || endsWith(buffer, len, "টা")
        || endsWith(buffer, len, "টি")
        || endsWith(buffer, len, "নি")
        || endsWith(buffer, len, "ের")
        || endsWith(buffer, len, "তে")
        || endsWith(buffer, len, "রা")
        || endsWith(buffer, len, "কে")
    ))
      return len - 2;

    // 1
    if ((len > 2) && (endsWith(buffer, len, "ি")
        || endsWith(buffer, len, "ী")
        || endsWith(buffer, len, "া")
        || endsWith(buffer, len, "ো")
        || endsWith(buffer, len, "ে")
        || endsWith(buffer, len, "ব")
        || endsWith(buffer, len, "ত")
    ))
      return len - 1;

    return len;
  }
}
