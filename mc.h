/**
 * Access objects using MinIO-mc header file.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <stdio.h>
#include <stdlib.h>

#include "config.h"

void init_minio_mc_config();
int copy_to_minio(const char *local, const char *remote);
int copy_from_minio(const char *local, const char *remote);
