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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;

/**
 * Downloads a file from a URL.
 *
 * @author Decebal Suiu
 */
public class SimpleFileDownloader implements FileDownloader {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDownloader.class);

    /**
     * Downloads a file. If HTTP(S) or FTP, stream content, if local file:/ do a simple filesystem copy to tmp folder.
     * Other protocols not supported.
     *
     * @param fileUrl the URI representing the file to download
     * @return the path of downloaded/copied file
     * @throws IOException in case of network or IO problems
     * @throws PluginException in case of other problems
     */
    public Path downloadFile(URL fileUrl) throws PluginException, IOException {
        switch (fileUrl.getProtocol()) {
            case "http":
            case "https":
            case "ftp":
                return downloadFileHttp(fileUrl);
            case "file":
                return copyLocalFile(fileUrl);
            default:
                throw new PluginException("URL protocol {} not supported", fileUrl.getProtocol());
        }
    }

    /**
     * Efficient copy of file in case of local file system.
     *
     * @param fileUrl source file
     * @return path of target file
     * @throws IOException if problems during copy
     * @throws PluginException in case of other problems
     */
    protected Path copyLocalFile(URL fileUrl) throws IOException, PluginException {
        Path destination = Files.createTempDirectory("pf4j-update-downloader");
        destination.toFile().deleteOnExit();

        try {
            Path fromFile = Paths.get(fileUrl.toURI());
            String path = fileUrl.getPath();
            String fileName = path.substring(path.lastIndexOf('/') + 1);
            Path toFile = destination.resolve(fileName);
            Files.copy(fromFile, toFile, StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING);

            return toFile;
        } catch (URISyntaxException e) {
            throw new PluginException("Something wrong with given URL", e);
        }
    }

    /**
     * Downloads file from HTTP or FTP.
     *
     * @param fileUrl source file
     * @return path of downloaded file
     * @throws IOException if IO problems
     * @throws PluginException if validation fails or any other problems
     */
    protected Path downloadFileHttp(URL fileUrl) throws IOException, PluginException {
        Path destination = Files.createTempDirectory("pf4j-update-downloader");
        destination.toFile().deleteOnExit();

        String path = fileUrl.getPath();
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        Path file = destination.resolve(fileName);

        // set up the URL connection
        URLConnection connection = fileUrl.openConnection();

        // connect to the remote site (may takes some time)
        connection.connect();

        // check for http authorization
        HttpURLConnection httpConnection = (HttpURLConnection) connection;
        if (httpConnection.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
            throw new ConnectException("HTTP Authorization failure");
        }

        // try to get the server-specified last-modified date of this artifact
        long lastModified = httpConnection.getHeaderFieldDate("Last-Modified", System.currentTimeMillis());

        // try to get the input stream (three times)
        InputStream is = null;
        for (int i = 0; i < 3; i++) {
            try {
                is = connection.getInputStream();
                break;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        if (is == null) {
            throw new ConnectException("Can't get '" + fileUrl + " to '" + file + "'");
        }

        // reade from remote resource and write to the local file
        FileOutputStream fos = new FileOutputStream(file.toFile());
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) >= 0) {
            fos.write(buffer, 0, length);
        }
        fos.close();
        is.close();

        log.debug("Set last modified of '{}' to '{}'", file, lastModified);
        Files.setLastModifiedTime(file, FileTime.fromMillis(lastModified));

        return file;
    }
}
