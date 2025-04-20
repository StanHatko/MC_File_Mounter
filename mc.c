/**
 * Access objects using MinIO-mc implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "mc.h"

char *mc_binary;
char *mc_prefix;

void init_minio_mc_config()
{
    mc_binary = getenv("mc_binary");
    validate_config_len("mc_binary", mc_binary, MAX_LEN_COMPONENTS);

    mc_prefix = getenv("mc_prefix");
    validate_config_len("mc_prefix", mc_prefix, MAX_LEN_COMPONENTS);
}

int copy_to_minio(const char *local, const char *remote)
{
    char cmd[MINIO_CMD_BUF_LEN];
    // TODO: adjust this!
    sprintf(cmd, "mc cp \"%s\" \"%s/%s\"", local, mc_prefix, remote);
    return system(cmd);
}

int copy_from_minio(const char *remote, const char *local)
{
    char cmd[MINIO_CMD_BUF_LEN];
    // TODO: adjust this!
    sprintf(cmd, "mc cp \"%s/%s\" \"%s\"", mc_prefix, remote, local);
    return system(cmd);
}
