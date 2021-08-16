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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

/**
 * {@link BackupRepository} implementation that stores files in Google Cloud Storage ("GCS").
 */
public class GCSBackupRepository implements BackupRepository {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE = 5 * 1024 * 1024;
    private static final Storage.BlobWriteOption[] NO_WRITE_OPTIONS = new Storage.BlobWriteOption[0];

    protected Storage storage;

    private NamedList<Object> config = null;
    protected String bucketName = null;
    protected String credentialPath = null;
    protected int writeBufferSizeBytes;
    protected int readBufferSizeBytes;
    protected StorageOptions.Builder storageOptionsBuilder = null;

    protected Storage initStorage() {
        if (storage != null)
            return storage;

        try {
            if (credentialPath == null) {
                throw new IllegalArgumentException(GCSConfigParser.missingCredentialErrorMsg());
            }

            log.info("Creating GCS client using credential at {}", credentialPath);
            // 'GoogleCredentials.fromStream' closes the input stream, so we don't
            GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(credentialPath));
            storageOptionsBuilder.setCredentials(credential);
            storage = storageOptionsBuilder.build().getService();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return storage;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void init(NamedList args) {
        this.config = (NamedList<Object>) args;
        final GCSConfigParser configReader = new GCSConfigParser();
        final GCSConfigParser.GCSConfig parsedConfig = configReader.parseConfiguration(config);

        this.bucketName = parsedConfig.getBucketName();
        this.credentialPath = parsedConfig.getCredentialPath();
        this.writeBufferSizeBytes = parsedConfig.getWriteBufferSize();
        this.readBufferSizeBytes = parsedConfig.getReadBufferSize();
        this.storageOptionsBuilder = parsedConfig.getStorageOptionsBuilder();

        initStorage();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConfigProperty(String name) {
        return (T) this.config.get(name);
    }

    @Override
    public URI createURI(String location) {
        Objects.requireNonNull(location);

        URI result;
        try {
            result = new URI(location);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Error on creating URI", e);
        }

        return result;
    }

    @Override
    public URI createDirectoryURI(String location) {
        Objects.requireNonNull(location);

        if (!location.endsWith("/")) {
            location += "/";
        }

        return createURI(location);
    }

    @Override
    public URI resolve(URI baseUri, String... pathComponents) {
        StringBuilder builder = new StringBuilder(baseUri.toString());
        for (String path : pathComponents) {
            if (path != null && !path.isEmpty()) {
                if (builder.charAt(builder.length()-1) != '/') {
                    builder.append('/');
                }
                builder.append(path);
            }
        }

        return URI.create(builder.toString()).normalize();
    }

    @Override
    public URI resolveDirectory(URI baseUri, String... pathComponents) {
        if (pathComponents.length > 0) {
            if (!pathComponents[pathComponents.length - 1].endsWith("/")) {
                pathComponents[pathComponents.length - 1] = pathComponents[pathComponents.length - 1] + "/";
            }
        } else {
            if (!baseUri.getPath().endsWith("/")) {
                baseUri = URI.create(baseUri + "/");
            }
        }
        return resolve(baseUri, pathComponents);
    }

    @Override
    public boolean exists(URI path) throws IOException {
        if (path.toString().equals(getConfigProperty(CoreAdminParams.BACKUP_LOCATION))) {
            return true;
        }

        if (path.toString().endsWith("/")) {
            return storage.get(bucketName, path.toString(), Storage.BlobGetOption.fields()) != null;
        } else {
            final String filePath = path.toString();
            final String directoryPath = path.toString() + "/";
            return storage.get(bucketName, filePath, Storage.BlobGetOption.fields()) != null ||
                    storage.get(bucketName, directoryPath, Storage.BlobGetOption.fields()) != null;
        }

    }

    @Override
    public PathType getPathType(URI path) throws IOException {
        if (path.toString().endsWith("/"))
            return PathType.DIRECTORY;

        Blob blob = storage.get(bucketName, path.toString()+"/", Storage.BlobGetOption.fields());
        if (blob != null)
            return PathType.DIRECTORY;

        return PathType.FILE;
    }

    @Override
    public String[] listAll(URI path) throws IOException {
        final String blobName = appendTrailingSeparatorIfNecessary(path.toString());

        final String pathStr = blobName;
        final LinkedList<String> result = new LinkedList<>();
        storage.list(
                bucketName,
                Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(pathStr),
                Storage.BlobListOption.fields())
        .iterateAll().forEach(
                blob -> {
                    assert blob.getName().startsWith(pathStr);
                    final String suffixName = blob.getName().substring(pathStr.length());
                    if (!suffixName.isEmpty()) {
                        // Remove trailing '/' if present
                        if (suffixName.endsWith("/")) {
                            result.add(suffixName.substring(0, suffixName.length() - 1));
                        } else {
                            result.add(suffixName);
                        }
                    }
                });

        return result.toArray(new String[0]);
    }

    @Override
    public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
        return openInput(dirPath, fileName, ctx, readBufferSizeBytes);
    }

    private IndexInput openInput(URI dirPath, String fileName, IOContext ctx, int bufferSize) {
        String blobName = resolve(dirPath, fileName).toString();

        final BlobId blobId = BlobId.of(bucketName, blobName);
        final Blob blob = storage.get(blobId, Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
        final ReadChannel readChannel = blob.reader();
        readChannel.setChunkSize(bufferSize);

        return new BufferedIndexInput(blobName, bufferSize) {

            @Override
            public long length() {
                return blob.getSize();
            }

            @Override
            protected void readInternal(ByteBuffer b) throws IOException {
                readChannel.read(b);
            }

            @Override
            protected void seekInternal(long pos) throws IOException {
                readChannel.seek(pos);
            }

            @Override
            public void close() throws IOException {
                readChannel.close();
            }
        };
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, path.toString()).build();
        final WriteChannel writeChannel = storage.writer(blobInfo, getDefaultBlobWriteOptions());

        return Channels.newOutputStream(new WritableByteChannel() {
            @Override
            public int write(ByteBuffer src) throws IOException {
                return writeChannel.write(src);
            }

            @Override
            public boolean isOpen() {
                return writeChannel.isOpen();
            }

            @Override
            public void close() throws IOException {
                writeChannel.close();
            }
        });
    }

    @Override
    public void createDirectory(URI path) throws IOException {
        final String name = appendTrailingSeparatorIfNecessary(path.toString());
        storage.create(BlobInfo.newBuilder(bucketName, name).build()) ;
    }

    @Override
    public void deleteDirectory(URI path) throws IOException {
        List<BlobId> blobIds = allBlobsAtDir(path);
        if (!blobIds.isEmpty()) {
            storage.delete(blobIds);
        } else {
            log.debug("Path:{} doesn't have any blobs", path);
        }
    }

    protected List<BlobId> allBlobsAtDir(URI path) throws IOException {
        final String blobName = appendTrailingSeparatorIfNecessary(path.toString());

        final List<BlobId> result = new ArrayList<>();
        final String pathStr = blobName;
        storage.list(
                bucketName,
                Storage.BlobListOption.prefix(pathStr),
                Storage.BlobListOption.fields())
        .iterateAll().forEach(
                blob -> result.add(blob.getBlobId())
        );

        return result;

    }

    @Override
    public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
        if (files.isEmpty()) {
            return;
        }
        final String prefix = appendTrailingSeparatorIfNecessary(path.toString());
        List<BlobId> blobDeletes = files.stream()
                .map(file -> BlobId.of(bucketName, prefix + file))
                .collect(Collectors.toList());
        List<Boolean> result = storage.delete(blobDeletes);
        if (!ignoreNoSuchFileException) {
            int failedDelete = result.indexOf(Boolean.FALSE);
            if (failedDelete != -1) {
                throw new NoSuchFileException("File " + blobDeletes.get(failedDelete).getName() + " was not found");
            }
        }
    }

    @Override
    public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI destDir, String destFileName) throws IOException {
        String blobName = destDir.toString();
        blobName = appendTrailingSeparatorIfNecessary(blobName);
        blobName += destFileName;
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobName).build();
        try (ChecksumIndexInput input = sourceDir.openChecksumInput(sourceFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
            if (input.length() <= CodecUtil.footerLength()) {
                throw new CorruptIndexException("file is too small:" + input.length(), input);
            }
            if (input.length() > LARGE_BLOB_THRESHOLD_BYTE_SIZE) {
                writeBlobResumable(blobInfo, input);
            } else {
                writeBlobMultipart(blobInfo, input, (int) input.length());
            }
        }
    }

    @Override
    public void copyIndexFileTo(URI sourceRepo, String sourceFileName, Directory dest, String destFileName) throws IOException {
        try {
            String blobName = sourceRepo.toString();
            blobName = appendTrailingSeparatorIfNecessary(blobName);
            blobName += sourceFileName;
            final BlobId blobId = BlobId.of(bucketName, blobName);
            try (final ReadChannel readChannel = storage.reader(blobId);
                 IndexOutput output = dest.createOutput(destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
                ByteBuffer buffer = ByteBuffer.allocate(readBufferSizeBytes);
                while (readChannel.read(buffer) > 0) {
                    buffer.flip();
                    byte[] arr = buffer.array();
                    output.writeBytes(arr, buffer.position(), buffer.limit() - buffer.position());
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            log.info("Here's an exception e", e);
        }
    }


    @Override
    public void close() throws IOException {

    }

    private void writeBlobMultipart(BlobInfo blobInfo, ChecksumIndexInput indexInput, int blobSize)
            throws IOException {
        byte[] bytes = new byte[blobSize];
        indexInput.readBytes(bytes, 0, blobSize - CodecUtil.footerLength());
        long checksum = CodecUtil.checkFooter(indexInput);
        ByteBuffer footerBuffer = ByteBuffer.wrap(bytes, blobSize - CodecUtil.footerLength(), CodecUtil.footerLength());
        writeFooter(checksum, footerBuffer);
        try {
            storage.create(blobInfo, bytes, Storage.BlobTargetOption.doesNotExist());
        } catch (final StorageException se) {
            if (se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

    private void writeBlobResumable(BlobInfo blobInfo, ChecksumIndexInput indexInput) throws IOException {
        try {
            final WriteChannel writeChannel = storage.writer(blobInfo, getDefaultBlobWriteOptions());

            ByteBuffer buffer = ByteBuffer.allocate(writeBufferSizeBytes);
            writeChannel.setChunkSize(writeBufferSizeBytes);

            long remain = indexInput.length() - CodecUtil.footerLength();
            while (remain > 0) {
                // reading
                int byteReads = (int) Math.min(buffer.capacity(), remain);
                indexInput.readBytes(buffer.array(), 0, byteReads);
                buffer.position(byteReads);
                buffer.flip();

                // writing
                writeChannel.write(buffer);
                buffer.clear();
                remain -= byteReads;
            }
            long checksum = CodecUtil.checkFooter(indexInput);
            ByteBuffer bytes = getFooter(checksum);
            writeChannel.write(bytes);
            writeChannel.close();
        } catch (final StorageException se) {
            if (se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

    private ByteBuffer getFooter(long checksum) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(CodecUtil.footerLength());
        writeFooter(checksum, buffer);
        return buffer;
    }

    private void writeFooter(long checksum, ByteBuffer buffer) throws IOException {
        IndexOutput out = new IndexOutput("", "") {

            @Override
            public void writeByte(byte b) throws IOException {
                buffer.put(b);
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                buffer.put(b, offset, length);
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public long getFilePointer() {
                return 0;
            }

            @Override
            public long getChecksum() throws IOException {
                return checksum;
            }
        };
        CodecUtil.writeFooter(out);
        buffer.flip();
    }

    protected Storage.BlobWriteOption[] getDefaultBlobWriteOptions() {
        return NO_WRITE_OPTIONS;
    }

    private String appendTrailingSeparatorIfNecessary(String blobName) {
        if (! blobName.endsWith("/")) {
            return blobName + "/";
        }
        return blobName;
    }
}
