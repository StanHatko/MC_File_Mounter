/**
 * Maintain list of currently open files header file.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define MAX_OPEN_FILES 1024

struct open_file_entry
{
    char *mc_filename;
    char *local_filename;
    int handles_count;
    pthread_mutex_t is_locked;
    bool is_open;
    bool need_copy_end;
};

int check_if_open(const char *mc_filename);
int open_file(const char *mc_filename);
