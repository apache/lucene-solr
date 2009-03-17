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

package org.apache.solr.common.util;

import java.io.File;
import java.io.IOException;

/**
 * @version $Id$
 */
public class FileUtils {

  /**
   * Resolves a path relative a base directory.
   *
   * <p>
   * This method does what "new File(base,path)" <b>Should</b> do, it it wasn't 
   * completley lame: If path is absolute, then a File for that path is returned; 
   * if it's not absoluve, then a File is returnd using "path" as a child 
   * of "base") 
   * </p>
   */
  public static File resolvePath(File base, String path) throws IOException {
    File r = new File(path);
    return r.isAbsolute() ? r : new File(base, path);
  }

}
