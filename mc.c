/**
 * Access objects using MinIO-mc implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "mc.h"

char *mc_binary;
int mc_mount_prefix_len;
char *mc_mount_prefix;
char *mc_request_prefix;

void init_minio_mc_config()
{
    mc_binary = getenv("mc_binary");
    print_config_var("mc_binary", mc_binary);
    validate_config_len("mc_binary", mc_binary, MAX_LEN_COMPONENTS);

    mc_mount_prefix = getenv("mc_mount_prefix");
    print_config_var("mc_mount_prefix", mc_mount_prefix);
    validate_config_len("mc_mount_prefix", mc_mount_prefix, MAX_LEN_COMPONENTS);
    mc_mount_prefix_len = strlen(mc_mount_prefix);

    mc_request_prefix = getenv("mc_request_prefix");
    print_config_var("mc_request_prefix", mc_request_prefix);
    validate_config_len("mc_request_prefix", mc_request_prefix, MAX_LEN_COMPONENTS);
}

// Remove mount prefix in front of path to MinIO file.
const char *remove_mount_prefix(const char *minio_path)
{
    int nc = strlen(minio_path);
    if (nc < mc_mount_prefix_len)
        return NULL;

    if (strncmp(mc_mount_prefix, minio_path, mc_mount_prefix_len) != 0)
        return NULL;

    return minio_path + mc_mount_prefix_len;
}

int copy_to_minio(const char *local, const char *remote)
{
    char cmd[MINIO_CMD_BUF_LEN];

    const char *r = remove_mount_prefix(remote);
    if (r == NULL)
    {
        fprintf(stderr, "Invalid destination path.");
        return -1;
    }

    // TODO: REPLACE BY posix_spawn which avoids shell escaping
    sprintf(cmd, "mc cp \"%s\" \"%s/%s\"", local, mc_request_prefix, r);
    return system(cmd);
}

int copy_from_minio(const char *remote, const char *local)
{
    char cmd[MINIO_CMD_BUF_LEN];

    const char *r = remove_mount_prefix(remote);
    if (r == NULL)
    {
        fprintf(stderr, "Invalid destination path.");
        return -1;
    }

    // TODO: REPLACE BY posix_spawn which avoids shell escaping
    sprintf(cmd, "mc cp \"%s/%s\" \"%s\"", mc_request_prefix, remote, local);
    return system(cmd);
}
