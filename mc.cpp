/**
 * Access objects using MinIO-mc implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

extern "C"
{
#include "common.h"
#include "mc.h"
}

std::string remove_mount_prefix(std::string minio_path);

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
    mc_mount_prefix_len = std::strlen(mc_mount_prefix);

    mc_request_prefix = getenv("mc_request_prefix");
    print_config_var("mc_request_prefix", mc_request_prefix);
    validate_config_len("mc_request_prefix", mc_request_prefix, MAX_LEN_COMPONENTS);
}

// Remove mount prefix in front of path to MinIO file.
std::string remove_mount_prefix(const char *minio_path)
{
    std::string r = minio_path;
    if (r.length() < mc_mount_prefix_len)
    {
        throw std::string("minio_path not in mount directory");
    }
    r.replace(0, mc_mount_prefix_len, "");
    int nc = strlen(minio_path);
    if (nc < mc_mount_prefix_len)
        return NULL;

    if (strncmp(mc_mount_prefix, minio_path, mc_mount_prefix_len) != 0)
        return NULL;

    return minio_path + mc_mount_prefix_len;
}

int copy_to_minio(const char *local, const char *remote)
{
    try
    {
        std::string cmd = mc_binary;
        std::string r = remove_mount_prefix(remote);

        // TODO: REPLACE BY posix_spawn which avoids shell escaping
        sprintf(cmd, "mc cp \"%s\" \"%s/%s\"", local, mc_request_prefix, r);
        return system(cmd);
    }
    catch (const std::string &e)
    {
        std::cerr << "Exception occurred:" << e << "\n";
        return -1; // TODO adjust
    }
}

int copy_from_minio(const char *remote, const char *local)
{
    // TODO implement
    return -1;
}
