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

#define TEMP_PATH_BUF_BASE_SIZE 96
#define TEMP_PATH_BUF_FULL_SIZE 128

// Prefix of temporary files path.
char *temp_files_prefix;

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
    sprintf(buf, "%s_%" PRIu64 "_data", temp_files_prefix, get_temp_file_num());
}

// Log operation that is performed.
void log_operation(const char *op_name)
{
    fprintf(stderr, "Perform operation: %s\n", op_name);
}

// Initialization function, sets up specified configuration.
void init_config()
{
    temp_files_prefix = getenv("temp_files_prefix");

    if (temp_files_prefix == NULL)
    {
        fprintf(stderr, "Must specify environment variable temp_files_prefix!\n");
        exit(1);
    }
    fprintf(stderr, "Using temp_files_prefix: %s", temp_files_prefix);

    int max_len = TEMP_PATH_BUF_BASE_SIZE - 64;
    int nt = strlen(temp_files_prefix);
    if (nt < max_len)
    {
        fprintf(stderr, "Too long temp_files_prefix, maximum is %d, specified %d!", max_len, nt);
        exit(1);
    }
}

// Main handler function that invokes C++ program that actually accesses the files.
int invoke_handler(const char *op, const char *temp_file_base)
{
    // TODO implement
    return -1;
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
