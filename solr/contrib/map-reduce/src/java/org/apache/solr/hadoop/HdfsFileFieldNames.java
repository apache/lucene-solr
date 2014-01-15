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
package org.apache.solr.hadoop;


/**
 * Solr field names for metadata of an HDFS file.
 */
public interface HdfsFileFieldNames {

  public static final String FILE_UPLOAD_URL = "file_upload_url";
  public static final String FILE_DOWNLOAD_URL = "file_download_url";
  public static final String FILE_SCHEME = "file_scheme";
  public static final String FILE_HOST = "file_host";
  public static final String FILE_PORT = "file_port";
  public static final String FILE_PATH = "file_path";
  public static final String FILE_NAME = "file_name";
  public static final String FILE_LENGTH = "file_length";
  public static final String FILE_LAST_MODIFIED = "file_last_modified";
  public static final String FILE_OWNER = "file_owner";
  public static final String FILE_GROUP = "file_group";
  public static final String FILE_PERMISSIONS_USER = "file_permissions_user";
  public static final String FILE_PERMISSIONS_GROUP = "file_permissions_group";
  public static final String FILE_PERMISSIONS_OTHER = "file_permissions_other";
  public static final String FILE_PERMISSIONS_STICKYBIT = "file_permissions_stickybit";

}
