/**
 * Access objects using MinIO-mc implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include "mc.h"

char *mc_binary;
char *mc_prefix;

// Ensure environment variable of OK length.
void validate_config_len(const char *name, const char *var, int max_len)
{
    if (var == NULL)
    {
        fprintf(stderr, "Variable %s cannot be NULL!", name);
        exit(1);
    }

    int nv = strlen(var);
    if (nv > max_len)
    {
        fprintf(stderr, "Variable %s too long at %d characters, maximum is %d!", name, nv, max_len);
        exit(1);
    }
}

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
