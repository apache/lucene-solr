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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompoundVerifier implements FileVerifier {
    /**
     * Default list of verifiers
     */
    public static final List<FileVerifier> ALL_DEFAULT_FILE_VERIFIERS = Arrays.asList(
                new BasicVerifier(),
                new Sha512SumVerifier());

    private List<FileVerifier> verifiers = new ArrayList<>();

    /**
     * Default constructor which will add the default verifiers to start with
     */
    public CompoundVerifier() {
        setVerifiers(ALL_DEFAULT_FILE_VERIFIERS);
    }

    /**
     * Constructs a Compound verifier using the supplied list of verifiers instead of the default ones
     * @param verifiers the list of verifiers to apply
     */
    public CompoundVerifier(List<FileVerifier> verifiers) {
        this.verifiers = verifiers;
    }

    /**
     * Verifies a plugin release using all configured {@link FileVerifier}s
     *
     * @param context the file verifier context object
     * @param file    the path to the downloaded file itself
     * @throws IOException     if there was a problem accessing file
     * @throws VerifyException in case of problems verifying the file
     */
    @Override
    public void verify(Context context, Path file) throws IOException, VerifyException {
        for (FileVerifier verifier : getVerifiers()) {
            verifier.verify(context, file);
        }
    }

    public List<FileVerifier> getVerifiers() {
        return verifiers;
    }

    public void setVerifiers(List<FileVerifier> verifiers) {
        this.verifiers = verifiers;
    }
}
