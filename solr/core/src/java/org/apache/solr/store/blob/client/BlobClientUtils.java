package org.apache.solr.store.blob.client;

import java.util.UUID;

/**
 * Utility class for the blob store client. 
 *
 * @author a.vuong
 * @since 218
 */
public class BlobClientUtils {
    
    public static final String BLOB_FILE_PATH_DELIMITER = "/";
    
    /**
     * Concatenate two valid string paths together with a forward slash delimiter
     */
    public static String concatenatePaths(String path1, String path2) {
        return path1 + (path2.startsWith(BLOB_FILE_PATH_DELIMITER) || 
                path1.endsWith(BLOB_FILE_PATH_DELIMITER) ? "" : BLOB_FILE_PATH_DELIMITER) + path2;
    }
    
    /**
     * Creates a new file path with a random name for the specified core
     * 
     * @param coreName name of core
     * @return filepath path to core files
     */
    public static String generateNewBlobCorePath(String coreName) {
        String randomBlobPath = UUID.randomUUID().toString();
        return BlobClientUtils.concatenatePaths(coreName, randomBlobPath);
    }
}
