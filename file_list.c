/**
 * Maintain list of currently open files implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include "file_list.h"

static struct open_file_entry list_open_files[MAX_OPEN_FILES];
static pthread_mutex_t open_file_lock = PTHREAD_MUTEX_INITIALIZER;

int check_if_open_nolock(const char *mc_filename)
{
    for (int i = 0; i < MAX_OPEN_FILES; i++)
    {
        struct open_file_entry s = list_open_files[i];
        if (s.is_open && (strcmp(s.mc_filename, mc_filename) == 0))
            return i;
    }
    return -1;
}

int check_if_open(const char *mc_filename)
{
    pthread_mutex_lock(&open_file_lock);
    int r = check_if_open_nolock(mc_filename);
    pthread_mutex_unlock(&open_file_lock);
    return r;
}
