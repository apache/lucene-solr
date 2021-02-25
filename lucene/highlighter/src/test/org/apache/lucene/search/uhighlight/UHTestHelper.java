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
package org.apache.lucene.search.uhighlight;

import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;

/** Helper for {@link UnifiedHighlighter} tests. */
class UHTestHelper {

  static final FieldType postingsType = new FieldType(TextField.TYPE_STORED);
  static final FieldType tvType = new FieldType(TextField.TYPE_STORED);
  static final FieldType postingsWithTvType = new FieldType(TextField.TYPE_STORED);
  static final FieldType reanalysisType = TextField.TYPE_STORED;

  static {
    postingsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    postingsType.freeze();

    tvType.setStoreTermVectors(true);
    tvType.setStoreTermVectorPositions(true);
    tvType.setStoreTermVectorOffsets(true);
    tvType.freeze();

    postingsWithTvType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    postingsWithTvType.setStoreTermVectors(true);
    postingsWithTvType.freeze();

    // re-analysis type needs no further changes.
  }

  public static FieldType randomFieldType(Random random, FieldType... typePossibilities) {
    if (typePossibilities == null || typePossibilities.length == 0) {
      typePossibilities =
          new FieldType[] {postingsType, tvType, postingsWithTvType, reanalysisType};
    }
    return typePossibilities[random.nextInt(typePossibilities.length)];
  }

  /** for {@link com.carrotsearch.randomizedtesting.annotations.ParametersFactory} */
  // https://github.com/carrotsearch/randomizedtesting/blob/master/examples/maven/src/main/java/com/carrotsearch/examples/randomizedrunner/Test007ParameterizedTests.java
  static Iterable<Object[]> parametersFactoryList() {
    return Arrays.asList(
        new Object[][] {{postingsType}, {tvType}, {postingsWithTvType}, {reanalysisType}});
  }
}
