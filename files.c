/**
 * Maintain list and contents of currently open files implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include "files.h"

static struct open_file_entry list_open_files[MAX_OPEN_FILES];
static pthread_mutex_t open_file_lock = PTHREAD_MUTEX_INITIALIZER;

// Check if file is in list of open files.
// Internal function that doesn't set lock.
int file_list_check_nolock(const char *mc_filename)
{
    for (int i = 0; i < MAX_OPEN_FILES; i++)
    {
        struct open_file_entry *s = &(list_open_files[i]);
        if (s->is_active && (strcmp(s->mc_filename, mc_filename) == 0))
            return i;
    }
    return -1;
}

// Check if file is in list of open files.
// Return index if yes, -1 if no.
int file_list_check(const char *mc_filename)
{
    pthread_mutex_lock(&open_file_lock);
    int r = file_list_check_nolock(mc_filename);
    pthread_mutex_unlock(&open_file_lock);
    return r;
}

// Check if file is in list of open files, if not, add it.
// Return index if successful, -1 if failed.
int file_list_add(const char *mc_filename)
{
    pthread_mutex_lock(&open_file_lock);

    // If already in list, return that.
    int r = file_list_check_nolock(mc_filename);
    if (r >= 0)
    {
        pthread_mutex_unlock(&open_file_lock);
        return r;
    }

    // Need to add to list.
    for (int i = 0; i < MAX_OPEN_FILES; i++)
    {
        // Find available entry.
        struct open_file_entry *s = &(list_open_files[i]);
        if (s->is_active)
            continue;

        // Found entry that's available.
        strcpy(s->mc_filename, mc_filename); // Earlier validated that mc_filename is not too long.
        s->is_active = true;
        s->is_init = false;
        pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        s->lock = lock;

        // Return entry for file.
        pthread_mutex_unlock(&open_file_lock);
        return i;
    }

    // Failed since no available entries.
    pthread_mutex_unlock(&open_file_lock);
    return -1;
}
