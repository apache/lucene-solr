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

package org.apache.lucene.index;

/**
 * This exception is thrown when Lucene detects
 * an index that is too old for this Lucene version
 */
public class IndexFormatTooOldException extends CorruptIndexException {

  public IndexFormatTooOldException(String filename, String version) {
    super("Format version is not supported" + (filename!=null ? (" in file '" + filename + "'") : "") +
        ": " + version + ". This version of Lucene only supports indexes created with release 3.0 and later.");
  }
  
  public IndexFormatTooOldException(String filename, int version, int minVersion, int maxVersion) {
    super("Format version is not supported" + (filename!=null ? (" in file '" + filename + "'") : "") +
        ": " + version + " (needs to be between " + minVersion + " and " + maxVersion +
    "). This version of Lucene only supports indexes created with release 3.0 and later.");
  }

}
