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

package org.apache.solr.handler.dataimport;

/**
 * <p>
 * Exception class for all DataImportHandler exceptions
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 * <p/>
 * $Id$
 *
 * @since solr 1.3
 */
public class DataImportHandlerException extends RuntimeException {
  private int errCode;

  public boolean debugged = false;

  public static final int SEVERE = 500, WARN = 400, SKIP = 300;

  public DataImportHandlerException(int err) {
    super();
    errCode = err;
  }

  public DataImportHandlerException(int err, String message) {
    super(message + MSG + SolrWriter.getDocCount());
    errCode = err;
  }

  public DataImportHandlerException(int err, String message, Throwable cause) {
    super(message + MSG + SolrWriter.getDocCount(), cause);
    errCode = err;
  }

  public DataImportHandlerException(int err, Throwable cause) {
    super(cause);
    errCode = err;
  }

  public int getErrCode() {
    return errCode;
  }

  public static final String MSG = " Processing Documemt # ";
}
