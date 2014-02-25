package org.apache.lucene.analysis.hunspell2;

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

import java.util.regex.Pattern;

/**
 * Wrapper class representing a hunspell affix
 */
final class Affix {

  private char appendFlags[]; // continuation class flags
  private String strip;
  
  private Pattern conditionPattern;
  
  private char flag;

  private boolean crossProduct;

  /**
   * Checks whether the given text matches the conditional pattern on this affix
   *
   * @param text Text to check if it matches the affix's conditional pattern
   * @return {@code true} if the text meets the condition, {@code false} otherwise
   */
  public boolean checkCondition(CharSequence text) {
    return conditionPattern.matcher(text).matches();
  }

  /**
   * Returns the flags defined for the affix append
   *
   * @return Flags defined for the affix append
   */
  public char[] getAppendFlags() {
    return appendFlags;
  }

  /**
   * Sets the flags defined for the affix append
   *
   * @param appendFlags Flags defined for the affix append
   */
  public void setAppendFlags(char[] appendFlags) {
    this.appendFlags = appendFlags;
  }

  /**
   * Returns the stripping characters defined for the affix
   *
   * @return Stripping characters defined for the affix
   */
  public String getStrip() {
    return strip;
  }

  /**
   * Sets the stripping characters defined for the affix
   *
   * @param strip Stripping characters defined for the affix
   */
  public void setStrip(String strip) {
    this.strip = strip;
  }

  /**
   * Sets the condition that must be met before the affix can be applied
   *
   * @param pattern Condition as a regular expression pattern
   */
  public void setCondition(Pattern pattern) {
    this.conditionPattern = pattern;
  }

  /**
   * Returns the affix flag
   *
   * @return Affix flag
   */
  public char getFlag() {
    return flag;
  }

  /**
   * Sets the affix flag
   *
   * @param flag Affix flag
   */
  public void setFlag(char flag) {
    this.flag = flag;
  }

  /**
   * Returns whether the affix is defined as cross product
   *
   * @return {@code true} if the affix is cross product, {@code false} otherwise
   */
  public boolean isCrossProduct() {
    return crossProduct;
  }

  /**
   * Sets whether the affix is defined as cross product
   *
   * @param crossProduct Whether the affix is defined as cross product
   */
  public void setCrossProduct(boolean crossProduct) {
    this.crossProduct = crossProduct;
  }
}
