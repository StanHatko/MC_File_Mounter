/**
 * Maintain list and contents of currently open files header file.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"

struct open_file_entry
{
    char mc_filename[MC_PATH_BUF_LEN];
    char local_filename[TEMP_PATH_BUF_LEN];
    FILE *file;
    pthread_mutex_t lock;
    int handles_count;
    bool is_active;
    bool is_init;
    bool need_copy_end;
};

int file_list_check(const char *mc_filename);
int file_list_add(const char *mc_filename);
