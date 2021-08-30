Apache Solr - S3 Repository
===========================

This S3 repository is a backup repository implementation designed to provide backup/restore functionality to Amazon S3.

# Getting Started

Add this to your `solr.xml`:

```xml
    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="s3.bucket.name">BUCKET_NAME</str>
            <str name="s3.region">us-west-2</str>
        </repository>
    </backup>
```

This plugin uses the [default AWS credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html), so ensure that your credentials are set appropriately (e.g., via env var, or in `~/.aws/credentials`, etc.).

## Testing locally

To run / test locally, first spin up S3Mock:

    mkdir /tmp/s3
    docker run -p 9090:9090 --env initialBuckets=TEST_BUCKET -v /tmp/s3:/s3mockroot -t adobe/s3mock

Add this to your `solr.xml`:

```xml
    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="s3.endpoint">http://localhost:9090</str>
            <str name="s3.bucket.name">TEST_BUCKET</str>
            <str name="s3.region">us-east-1</str>
        </repository>
    </backup>
```

Start Solr, and create a collection (e.g., "foo"). Then hit the following URL, which will take a backup and persist it in S3Mock under the name `test`:

http://localhost:8983/solr/admin/collections?action=BACKUP&repository=s3&location=s3:/&collection=foo&name=test

To restore from that backup, hit this URL, which will create a new collection `bar` with the contents of the backup `test` you just made: 

http://localhost:8983/solr/admin/collections?action=RESTORE&repository=s3&location=s3:/&name=test&collection=bar

## Change the S3 Endpoint

If you are also running Solr in a docker image, and need to set the endpoint of S3Mock to be different than `localhost`, then add the following under `<repository>`:

```xml
    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="s3.bucket.name">TEST_BUCKET</str>
            <str name="s3.endpoint">http://host.docker.internal:9090</str>
            <str name="s3.region">us-east-1</str>
        </repository>
    </backup>
```

This works for the regular S3 backup repository as well (not mock).
But the plugin only provides official support for AWS S3, not _S3 compatible_ products.
Use this plugin with _S3 compatible_ products at your own risk.
Certain options, such as Minio, are known to be incompatible with this plugin.
