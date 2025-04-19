/**
 * Maintain list and contents of currently open files header file.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define MAX_OPEN_FILES 256
#define MC_PATH_BUF_LEN 296
#define TEMP_PATH_BUF_LEN 72

struct open_file_entry
{
    char mc_filename[MC_PATH_BUF_LEN];
    char local_filename[TEMP_PATH_BUF_LEN];
    int handles_count;
    pthread_mutex_t lock;
    bool is_active;
    bool is_init;
    bool need_copy_end;
};

int file_list_check(const char *mc_filename);
int file_list_add(const char *mc_filename);
