/**
 * Internal support functions for MinIO-mc mounting program.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "config.h"

// Prefix of temporary files path.
const char *temp_files_prefix;

// Prefix of MinIO data files path.
const char *mc_data_prefix;

// Get number that should be used for new temporary file.
uint64_t get_temp_file_num()
{
    static uint64_t cur_num = 0;
    static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lock);
    cur_num += 1;
    pthread_mutex_unlock(&lock);
    return cur_num;
}

// Get base name to use for temporary file.
void get_temp_file_base(char *buf)
{
    sprintf(buf, "%s_trans_%" PRIu64 "_data", temp_files_prefix, get_temp_file_num());
}

// Log operation that is performed.
void log_operation(const char *op_name)
{
    printf("Perform operation: %s\n", op_name);
}

// Log path.
void log_path(const char *name, const char *path)
{
    printf("Path %s: %s\n", name, path);
}

// Get and validate single configuration variable.
const char *get_config_var(const char *var_name, int max_len)
{
    const char *contents = getenv(var_name);

    if (contents == NULL)
    {
        printf("Must specify environment variable %s!\n", var_name);
        exit(1);
    }
    printf("Using %s: %s\n", var_name, contents);

    int nt = strlen(contents);
    if (nt > max_len)
    {
        printf("Too long %s, maximum is %d, specified %d!", var_name, max_len, nt);
        exit(1);
    }

    return contents;
}

// Initialization function, sets up specified configuration from environment variables.
void init_config()
{
    temp_files_prefix = get_config_var("temp_files_prefix", TEMP_PATH_BUF_BASE_SIZE - 64);
    mc_data_prefix = get_config_var("mc_data_prefix", MAX_PATH_LEN - 64);

    // Don't need in this program, but prevents other program from crashing due to missing environment variable.
    get_config_var("mc_bin_path", 255);
}

// Main handler function that invokes C++ program that actually accesses the files.
int invoke_handler(const char *op, const char *temp_file_base)
{
    char cmd[TEMP_PATH_BUF_FULL_SIZE + 256];
    sprintf(cmd, "./request_handler %s %s", op, temp_file_base);
    return system(cmd);
}

// Macro that outputs input file for operation parameters.
#define WRITE_OP_INPUT(dest_name, source_buf, size)                 \
    {                                                               \
        char temp_path_cur[TEMP_PATH_BUF_FULL_SIZE];                \
        sprintf(temp_path_cur, "%s.%s", temp_path_base, dest_name); \
        FILE *f = fopen(temp_path_cur, "wb");                       \
        if (f == NULL)                                              \
            return -1; /*TODO adjust*/                              \
        fwrite(source_buf, size, 1, f);                             \
        fclose(f);                                                  \
    }
