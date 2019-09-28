/*
 * Copyright (C) 2012-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.packagemanager.pf4j;


import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

/**
 * Interface to download a file.
 */
public interface FileDownloader {

    /**
     * Downloads a file to destination. The implementation should download to a temporary folder.
     * Implementations may choose to support different protocols such as http, https, ftp, file...
     * The path returned must be of temporary nature and will most probably be moved/deleted by consumer.
     *
     * @param fileUrl the URL representing the file to download
     * @return Path of downloaded file, typically in a temporary folder
     * @throws IOException if there was an IO problem during download
     * @throws PluginException in case of other problems, such as unsupported protocol
     */
    Path downloadFile(URL fileUrl) throws PluginException, IOException;
}
