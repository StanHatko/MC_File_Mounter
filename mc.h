/**
 * Access objects using MinIO-mc header file.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include "config.h"

void init_minio_mc_config();
const char *remove_mount_prefix(const char *minio_path);
int copy_to_minio(const char *local, const char *remote);
int copy_from_minio(const char *local, const char *remote);
