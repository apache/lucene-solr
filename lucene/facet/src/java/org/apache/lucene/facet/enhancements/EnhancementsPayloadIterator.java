package org.apache.lucene.facet.enhancements;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import org.apache.lucene.facet.search.PayloadIterator;
import org.apache.lucene.util.Vint8;
import org.apache.lucene.util.Vint8.Position;

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

/**
 * A {@link PayloadIterator} for iterating over category posting lists generated
 * using {@link EnhancementsCategoryTokenizer}.
 * 
 * @lucene.experimental
 */
public class EnhancementsPayloadIterator extends PayloadIterator {

  private CategoryEnhancement[] EnhancedCategories;
  int nEnhancements;
  private int[] enhancementLength;
  private int[] enhancementStart;

  /**
   * Constructor.
   * 
   * @param enhancementsList
   *            A list of the {@link CategoryEnhancement}s from the indexing
   *            params.
   * @param indexReader
   *            A reader of the index.
   * @param term
   *            The category term to iterate.
   * @throws IOException If there is a low-level I/O error.
   */
  public EnhancementsPayloadIterator(
      List<CategoryEnhancement> enhancementsList,
      IndexReader indexReader, Term term) throws IOException {
    super(indexReader, term);
    EnhancedCategories = enhancementsList
        .toArray(new CategoryEnhancement[enhancementsList.size()]);
    enhancementLength = new int[EnhancedCategories.length];
    enhancementStart = new int[EnhancedCategories.length];
  }

  @Override
  public boolean setdoc(int docId) throws IOException {
    if (!super.setdoc(docId)) {
      return false;
    }

    // read header - number of enhancements and their lengths
    Position position = new Position();
    nEnhancements = Vint8.decode(buffer, position);
    for (int i = 0; i < nEnhancements; i++) {
      enhancementLength[i] = Vint8.decode(buffer, position);
    }

    // set enhancements start points
    enhancementStart[0] = position.pos;
    for (int i = 1; i < nEnhancements; i++) {
      enhancementStart[i] = enhancementStart[i - 1] + enhancementLength[i - 1];
    }

    return true;
  }

  /**
   * Get the data of the current category and document for a certain
   * enhancement, or {@code null} if no such enhancement exists.
   * 
   * @param enhancedCategory
   *            The category enhancement to apply.
   * @return the data of the current category and document for a certain
   *         enhancement, or {@code null} if no such enhancement exists.
   */
  public Object getCategoryData(CategoryEnhancement enhancedCategory) {
    for (int i = 0; i < nEnhancements; i++) {
      if (enhancedCategory.equals(EnhancedCategories[i])) {
        return enhancedCategory.extractCategoryTokenData(buffer,
            enhancementStart[i], enhancementLength[i]);
      }
    }
    return null;
  }
}
