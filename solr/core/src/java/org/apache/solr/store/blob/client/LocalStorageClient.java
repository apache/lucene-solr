package org.apache.solr.store.blob.client;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.solr.store.blob.client.BlobException;
import org.apache.solr.store.blob.client.BlobstoreProviderType;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobClientUtils;
import org.apache.solr.store.blob.client.ToFromJson;

/**
 * Class that handles reads and writes of solr blob files to the local file system.
 */
public class LocalStorageClient implements CoreStorageClient {

    /** The directory on the local file system where blobs will be stored. */
    private final String blobStoreRootDir;
    
    /** The key that identifies a file as the blob core metadata */
    private final String blobCoreMetadataName;

    public LocalStorageClient(String blobStoreRootDir, String blobCoreMetadataName) throws Exception {
        this.blobStoreRootDir = blobStoreRootDir;
        this.blobCoreMetadataName = blobCoreMetadataName;
        File rootDir = new File(blobStoreRootDir);
        rootDir.mkdirs(); // Might create the directory... or not
        if (!rootDir.isDirectory()) {
            throw new IOException("Can't create local Blob root directory " + rootDir.getAbsolutePath());
        }
    }

    private File getCoreRootDir(String blobName) {
        return new File(blobStoreRootDir + blobName);
    }

    @Override
    public String pushStream(String blobName, InputStream is, long contentLength) throws BlobException {
        try {
            createCoreStorage(blobName);
            String randomBlobId = createNewNonExistingBlob(blobName);
            String blobPath = BlobClientUtils.concatenatePaths(blobName, randomBlobId); 
            
            Files.copy(is, Paths.get(getblobAbsolutePath(blobPath)), StandardCopyOption.REPLACE_EXISTING);
    
            assert new File(getblobAbsolutePath(blobPath)).length() == contentLength;
    
            return blobPath;
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    /**
     * Picks a unique name for a new blob for the given core.<p>
     * The current implementation creates a file, but eventually we just pick up a random blob name then delegate to S3...
     *
     */
    private String createNewNonExistingBlob(String blobName) throws BlobException {
        try {
            String randomBlobPath = UUID.randomUUID().toString();
            String blobPath = BlobClientUtils.concatenatePaths(blobName, randomBlobPath);
            final File blobFile = new File(getblobAbsolutePath(blobPath));
            if (blobFile.exists()) {
                // Not expecting this ever to happen. In theory we could just do "continue" here to try a new
                // name. For now throwing an exception to make sure we don't run into this...
                // continue;
                throw new IllegalStateException("The random file name chosen using UUID already exists. Very worrying! " + blobFile.getAbsolutePath());
            }

            return randomBlobPath;
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public InputStream pullStream(String blobPath) throws BlobException {
        try {
            File blobFile = new File(getblobAbsolutePath(blobPath));
            return new FileInputStream(blobFile);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public void pushCoreMetadata(String blobName, BlobCoreMetadata bcm) throws BlobException {
        try {
            createCoreStorage(blobName);
            ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
            String json = converter.toJson(bcm);
    
            // Constant path under which the core metadata is stored in the Blob store (the only blob stored under a constant path!)
            String blobMetadataPath = getblobAbsolutePath(getBlobMetadataName(blobName));
            final File blobMetadataFile = new File(blobMetadataPath); 
    
            // Writing to the file assumed atomic, the file cannot be observed midway. Might not hold here but should be the case
            // with a real S3 implementation.
            try (PrintWriter out = new PrintWriter(blobMetadataFile)){
                out.println(json);
            }  
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public BlobCoreMetadata pullCoreMetadata(String blobName) throws BlobException {
        try {
            if (!coreMetadataExists(blobName)) {
                return null;
            }
            
            String blobMetadataPath = getblobAbsolutePath(getBlobMetadataName(blobName));
            File blobMetadataFile = new File(blobMetadataPath); 
            
            String json = new String(Files.readAllBytes(blobMetadataFile.toPath()));
            ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
            return converter.fromJson(json, BlobCoreMetadata.class);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public boolean coreMetadataExists(String blobName) throws BlobException { 
        try {
            String blobMetadataPath = getblobAbsolutePath(getBlobMetadataName(blobName));
            File coreMetadataFile = new File(blobMetadataPath); 
            return coreMetadataFile.exists();
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    /**
     * Prefixes the given path with the blob store root directory on the local FS 
     */
    private String getblobAbsolutePath(String blobPath) {
        return BlobClientUtils.concatenatePaths(blobStoreRootDir, blobPath);
    }
    
    private void createCoreStorage(String blobName) throws Exception {
        File coreRootDir = getCoreRootDir(blobName);
        coreRootDir.mkdirs();
        if (!coreRootDir.isDirectory()) {
            throw new IOException("Can't create Blob core root directory " + coreRootDir.getAbsolutePath());
        }
    }
    
    @Override
    public void deleteCore(String blobName) throws BlobException {
        try {
            Path path = Paths.get(getblobAbsolutePath(blobName));
            Files.walk(path)
                // Since this traversal includes the root directory
                // we need to reverse because we can't delete non-empty directories 
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(file -> {
                    file.delete();
                });
        } catch (Exception ex) {
            // In case the path doesn't exist, we'll just swallow the exception because it's not an issue,
            // especially in test clean up.
        }
    }
    
    /**
     * On the local FS implementation we'll just delete blob files individually
     */
    @Override
    public void deleteBlobs(Collection<String> paths) throws BlobException {
        try {
            for (String blobPath : paths) {
                final File blobFile = new File(getblobAbsolutePath(blobPath));
                blobFile.delete();
            }
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    private String getBlobMetadataName(String blobName) {
        return BlobClientUtils.concatenatePaths(blobName, blobCoreMetadataName);
    }
    
    @Override
    public BlobstoreProviderType getStorageProvider() {
        return BlobstoreProviderType.LOCAL_FILE_SYSTEM;
    }
    
    @Override 
    public String getBucketRegion() {
        return "N/A";
    }
    
    @Override 
    public String getBucketName() {
        return "N/A";
    }
    
    @Override 
    public String getEndpoint() {
        return "N/A";
    }

    /**
     * The local file system is being used, there's nothing to connect to so this method will always return true.
     */
    @Override
    public boolean doesBucketExist() {
        return true;
    }

    @Override
    public void shutdown() {
        
    }
    
    @Override
    public List<String> listCoreBlobFilesOlderThan(String blobName, long timestamp) throws BlobException {
        try {
            Path path = Paths.get(getblobAbsolutePath(blobName));
            List<String> blobFiles =
                    Files.walk(path).map(Path::toFile)
                    // We need to ignore the root directory as a path since this traversal includes it
                    .filter(file -> (file.lastModified() < timestamp) && !file.isDirectory())
                    .map(file -> BlobClientUtils.concatenatePaths(blobName, file.getName()))
                    .collect(Collectors.toList());
            return blobFiles;
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    @Override
    public List<String> listCommonBlobPrefix(String prefix) throws BlobException {
        try {
            String rootBlobDir = getblobAbsolutePath("");
            Path path = Paths.get(rootBlobDir);
            List<String> blobFiles =
                    Files.walk(path).map(Path::toFile)
                    .filter(file -> (!file.isDirectory()))
                    .map(file -> {
                        // extracts just the file system blob file name without the root dir
                        String fileBlobKey = file.getAbsolutePath().substring(rootBlobDir.length());
                        // extract the common prefix up to the delimiter
                        return fileBlobKey.substring(0, fileBlobKey.indexOf(BlobClientUtils.BLOB_FILE_PATH_DELIMITER));
                    })
                    .distinct()
                    .collect(Collectors.toList());
            return blobFiles;
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
}
