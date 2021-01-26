package org.apache.solr.core.backup;

/**
 * Represents the ID of a particular backup point for a particular shard.
 *
 * ShardBackupId's only need be unique within a given collection and backup location/name, so in practice they're formed
 * by combining the shard name with the {@link BackupId} in the form: "md_$SHARDNAME_BACKUPID".
 *
 * ShardBackupId's are most often used as a filename to store shard-level metadata for the backup.  See
 * {@link ShardBackupMetadata} for more information.
 *
 * @see ShardBackupMetadata
 */
public class ShardBackupId {
    private static final String FILENAME_SUFFIX = ".json";
    private final String shardName;
    private final BackupId containingBackupId;

    public ShardBackupId(String shardName, BackupId containingBackupId) {
        this.shardName = shardName;
        this.containingBackupId = containingBackupId;
    }

    public String getShardName() {
        return shardName;
    }

    public BackupId getContainingBackupId() {
        return containingBackupId;
    }

    public String getIdAsString() {
        return "md_" + shardName + "_" + containingBackupId.getId();
    }

    public String getBackupMetadataFilename() {
        return getIdAsString() + FILENAME_SUFFIX;
    }

    public static ShardBackupId from(String idString) {
        final String[] idComponents = idString.split("_");
        if (idComponents.length != 3) {
            throw new IllegalArgumentException("Unable to parse invalid ShardBackupId: " + idString);
        }

        final BackupId containingBackupId = new BackupId(Integer.parseInt(idComponents[2]));
        return new ShardBackupId(idComponents[1], containingBackupId);
    }

    public static ShardBackupId fromShardMetadataFilename(String filenameString) {
        if (! filenameString.endsWith(FILENAME_SUFFIX)) {
            throw new IllegalArgumentException("'filenameString' arg [" + filenameString + "] does not appear to be a filename");
        }
        final String idString = filenameString.substring(0, filenameString.length() - FILENAME_SUFFIX.length());
        return from(idString);
    }
}
