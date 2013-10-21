package org.apache.lucene.analysis.ko.morph;

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
 * 결합이 가능한 조건을 처리하는 클래스
 */
class ConstraintUtil {
  private ConstraintUtil() {}
  
  static boolean canHaheCompound(String key) {
    return key.length() == 2 && ("민족".equals(key) || "동서".equals(key) || "남북".equals(key));
  }
  
  // 종성이 있는 음절과 연결될 수 없는 조사
  static boolean isTwoJosa(char josa) {
    switch (josa) {
      case '가':
      case '는':
      case '다':
      case '나':
      case '니':
      case '고':
      case '라':
      case '와':
      case '랑':
      case '를':
      case '며':
      case '든':
      case '야':
      case '여': return true;
      default: return false;
    }
  }
  
  // 종성이 없는 음절과 연결될 수 없는 조사
  static boolean isThreeJosa(char josa) {
    switch (josa) {
      case '과':
      case '은':
      case '아':
      case '으':
      case '을': return true;
      default: return false;
    }
  } 
}
