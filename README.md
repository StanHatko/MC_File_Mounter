[DRAFT] Implementation of Filesystem Mount for S3 Accessible with mc Command
=======================================

This is an implementation of mounting a filesystem with FUSE for an S3-compatible folder accessible using the MinIO mc command.
Based on sample FUSE repo https://github.com/MaaSTaaR/LSYSFS.
Most of the actual methods haven't been implemented yet.

License: GNU GPL.

## Testing on Localhost

Download MinIO:

```bash
wget https://dl.minio.io/server/minio/release/linux-amd64/minio
chmod +x minio
```

Start MinIO:

```bash
mkdir /tmp/data
./minio server /tmp/data
```

Setup MinIO bucket and environment (fill in the environment variables):

```bash
export minio_alias=
export minio_url=
export minio_username=
export minio_password=
export minio_bucket=

./mc alias set "$minio_alias" "$minio_url" "$minio_username" "$minio_password"
./mc mb "$minio_alias/$minio_bucket"
```

Run full test (fill in full path of desired area in MinIO, can be "subdirectory" within bucket):

```bash
export minio_files_path=

TODO
```
