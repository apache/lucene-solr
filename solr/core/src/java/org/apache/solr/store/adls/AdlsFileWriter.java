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
package org.apache.solr.store.adls;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;

import com.microsoft.azure.datalake.store.IfExists;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 */
public class AdlsFileWriter extends OutputStreamIndexOutput {
  public static final int BUFFER_SIZE = 16384;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public AdlsFileWriter(AdlsProvider client, String path, String name) throws IOException {
    super("fileSystem= ADLS path=" + path, name, getOutputStream(client, path), BUFFER_SIZE);
    log.info("create ADLS writer for file {} {}",path,name);
  }
  
  private static final OutputStream getOutputStream(AdlsProvider clientn, String path) throws IOException {
    return clientn.createFile(path, IfExists.FAIL);
  }

}
