package org.apache.solr.store.blob.util;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for BlobStore components
 */
public class BlobStoreUtils {
    
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	public static String buildBlobStoreMetadataName(String suffix) {
	  return BlobStorageProvider.CORE_METADATA_BLOB_FILENAME + "." + suffix;
	}
	
	/**
	 * Generates a metadataSuffix value that gets appended to the name of {@link BlobCoreMetadata} 
	 * that are pushed to blob store
	 */
	public static String generateMetadataSuffix() {
	  return UUID.randomUUID().toString();
	}
	
}
