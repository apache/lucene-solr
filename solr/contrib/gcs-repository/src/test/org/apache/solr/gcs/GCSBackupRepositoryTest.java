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

package org.apache.solr.gcs;

import com.google.common.collect.Lists;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

/**
 * Unit tests for {@link GCSBackupRepository} that use an in-memory Storage object
 */
public class GCSBackupRepositoryTest extends AbstractBackupRepositoryTest {

    // Locale langs unsupported by google-cloud-nio's 'Storage' drop-in.  May need added to as Jenkins finds fails.
    // (Note that the issue here is in the test-stub, actual GCS use is fine with these locales).
    private static final List<String> INCOMPATIBLE_LOCALE_LANGS = Lists.newArrayList("ar", "dz", "uz", "ne", "mzn", "pa",
            "sd", "mr", "ig", "as", "fa", "my", "bn", "lrc", "ur", "ks", "th", "ckb", "ja", "ps", "hi");

    @BeforeClass
    public static void ensureCompatibleLocale() {
        final String defaultLang = Locale.getDefault().getLanguage();

        assumeFalse("This test uses a GCS mock library that is incompatible with the current default locale " + defaultLang,
                INCOMPATIBLE_LOCALE_LANGS.contains(defaultLang));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        LocalStorageGCSBackupRepository.clearStashedStorage();
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected BackupRepository getRepository() {
        final NamedList<Object> config = new NamedList<>();
        config.add(CoreAdminParams.BACKUP_LOCATION, "backup1");
        final GCSBackupRepository repository = new LocalStorageGCSBackupRepository();
        repository.init(config);

        return repository;
    }

    @Override
    protected URI getBaseUri() throws URISyntaxException {
        return new URI("tmp");
    }
}
