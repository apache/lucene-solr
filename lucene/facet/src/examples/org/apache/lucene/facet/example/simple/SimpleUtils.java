package org.apache.lucene.facet.example.simple;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.facet.example.ExampleUtils;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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
 * Some definitions for the Simple Sample.
 * 
 * @lucene.experimental
 */
public class SimpleUtils {

  /** 
   * Documents text field.
   */
  public static final String TEXT = "text"; 

  /** 
   * Documents title field.
   */
  public static final String TITLE = "title";

  /** 
   * sample documents text (for the text field).
   */
  public static String[] docTexts = {
    "the white car is the one I want.",
    "the white dog does not belong to anyone.",
  };

  /** 
   * sample documents titles (for the title field).
   */
  public static String[] docTitles = {
    "white car",
    "white dog",
  };

  /**
   * Categories: categories[D][N] == category-path no. N for document no. D.
   */
  public static CategoryPath[][] categories = {
    { new CategoryPath("root","a","f1"), new CategoryPath("root","a","f2") },
    { new CategoryPath("root","a","f1"), new CategoryPath("root","a","f3") },
  };

  /**
   * Analyzer used in the simple sample.
   */
  public static final Analyzer analyzer = new WhitespaceAnalyzer(ExampleUtils.EXAMPLE_VER);

}
