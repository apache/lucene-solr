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
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Verifies that the file exists is a regular file and has a non-null size
 */
public class BasicVerifier implements FileVerifier {
    /**
     * Verifies a plugin release according to certain rules
     *
     * @param context the file verifier context object
     * @param file    the path to the downloaded file itself
     * @throws IOException     if there was a problem accessing file
     * @throws VerifyException in case of problems verifying the file
     */
    @Override
    public void verify(Context context, Path file) throws VerifyException, IOException {
        if (!Files.isRegularFile(file) || Files.size(file) == 0) {
            throw new VerifyException("File {} is not a regular file or has size 0", file);
        }
    }
}
