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

package org.apache.solr.core.backup;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.util.PropertiesInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.solr.common.cloud.DocCollection.STATE_FORMAT;

/**
 * Represents a backup[-*].properties file, responsible for holding whole-collection and whole-backup metadata.
 *
 * These files live in a different location and hold different metadata depending on the backup format used.  The (now
 * deprecated) traditional 'full-snapshot' backup format places this file at $LOCATION/$NAME, while the preferred
 * incremental backup format stores these files in $LOCATION/$NAME/$COLLECTION.
 */
public class BackupProperties {

    private double indexSizeMB;
    private int indexFileCount;

    private Properties properties;

    private BackupProperties(Properties properties) {
        this.properties = properties;
    }

    public static BackupProperties create(String backupName,
                                          String collectionName,
                                          String extCollectionName,
                                          String configName) {
        Properties properties = new Properties();
        properties.put(BackupManager.BACKUP_NAME_PROP, backupName);
        properties.put(BackupManager.COLLECTION_NAME_PROP, collectionName);
        properties.put(BackupManager.COLLECTION_ALIAS_PROP, extCollectionName);
        properties.put(CollectionAdminParams.COLL_CONF, configName);
        properties.put(BackupManager.START_TIME_PROP, Instant.now().toString());
        properties.put(BackupManager.INDEX_VERSION_PROP, Version.LATEST.toString());

        return new BackupProperties(properties);
    }

    public static Optional<BackupProperties> readFromLatest(BackupRepository repository, URI backupPath) throws IOException {
        Optional<BackupId> lastBackupId = BackupFilePaths.findMostRecentBackupIdFromFileListing(repository.listAllOrEmpty(backupPath));

        if (!lastBackupId.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(readFrom(repository, backupPath, BackupFilePaths.getBackupPropsName(lastBackupId.get())));
    }

    public static BackupProperties readFrom(BackupRepository repository, URI backupPath, String fileName) throws IOException {
        Properties props = new Properties();
        try (Reader is = new InputStreamReader(new PropertiesInputStream(
                repository.openInput(backupPath, fileName, IOContext.DEFAULT)), StandardCharsets.UTF_8)) {
            props.load(is);
            return new BackupProperties(props);
        }
    }

    public List<String> getAllShardBackupMetadataFiles() {
        return properties.entrySet()
                .stream()
                .filter(entry -> entry.getKey().toString().endsWith(".md"))
                .map(entry -> entry.getValue().toString())
                .collect(Collectors.toList());
    }

    public void countIndexFiles(int numFiles, double sizeMB) {
        indexSizeMB += sizeMB;
        indexFileCount += numFiles;
    }

    public Optional<ShardBackupId> getShardBackupIdFor(String shardName) {
        String key = getKeyForShardBackupId(shardName);
        if (properties.containsKey(key)) {
            return Optional.of(ShardBackupId.fromShardMetadataFilename(properties.getProperty(key)));
        }
        return Optional.empty();
    }

    public ShardBackupId putAndGetShardBackupIdFor(String shardName, int backupId) {
        final ShardBackupId shardBackupId = new ShardBackupId(shardName, new BackupId(backupId));
        properties.put(getKeyForShardBackupId(shardName), shardBackupId.getBackupMetadataFilename());
        return shardBackupId;
    }

    private String getKeyForShardBackupId(String shardName) {
        return shardName+".md";
    }


    public void store(Writer propsWriter) throws IOException {
        properties.put("indexSizeMB", String.valueOf(indexSizeMB));
        properties.put("indexFileCount", String.valueOf(indexFileCount));
        properties.store(propsWriter, "Backup properties file");
    }

    public String getCollection() {
        return properties.getProperty(BackupManager.COLLECTION_NAME_PROP);
    }


    public String getCollectionAlias() {
        return properties.getProperty(BackupManager.COLLECTION_ALIAS_PROP);
    }

    public String getConfigName() {
        return properties.getProperty(CollectionAdminParams.COLL_CONF);
    }

    public String getStartTime() {
        return properties.getProperty(BackupManager.START_TIME_PROP);
    }

    public String getIndexVersion() {
        return properties.getProperty(BackupManager.INDEX_VERSION_PROP);
    }

    public String getStateFormat() {
        return properties.getProperty(STATE_FORMAT);
    }

    public Map<Object, Object> getDetails() {
        Map<Object, Object> result = new HashMap<>(properties);
        result.remove(BackupManager.BACKUP_NAME_PROP);
        result.remove(BackupManager.COLLECTION_NAME_PROP);
        result.put("indexSizeMB", Double.valueOf(properties.getProperty("indexSizeMB")));
        result.put("indexFileCount", Integer.valueOf(properties.getProperty("indexFileCount")));

        Map<String, String> shardBackupIds = new HashMap<>();
        Iterator<Object> keyIt = result.keySet().iterator();
        while (keyIt.hasNext()) {
            String key = keyIt.next().toString();
            if (key.endsWith(".md")) {
                shardBackupIds.put(key.substring(0, key.length() - 3), properties.getProperty(key));
                keyIt.remove();
            }
        }
        result.put("shardBackupIds", shardBackupIds);
        return result;
    }

    public String getBackupName() {
        return properties.getProperty(BackupManager.BACKUP_NAME_PROP);
    }
}
