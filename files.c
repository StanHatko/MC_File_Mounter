/**
 * Maintain list and contents of currently open files implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include "files.h"

static struct open_file_entry files_list[MAX_OPEN_FILES];
static pthread_mutex_t file_list_lock = PTHREAD_MUTEX_INITIALIZER;

// Check if file is in list of open files.
// Internal function that doesn't set lock.
int file_list_check_nolock(const char *mc_filename)
{
    for (int i = 0; i < MAX_OPEN_FILES; i++)
    {
        struct open_file_entry *s = &(files_list[i]);
        if (s->is_active && (strcmp(s->mc_filename, mc_filename) == 0))
            return i;
    }
    return -1;
}

// Check if file is in list of open files.
// Return index if yes, -1 if no.
int file_list_check(const char *mc_filename)
{
    pthread_mutex_lock(&file_list_lock);
    int r = file_list_check_nolock(mc_filename);
    pthread_mutex_unlock(&file_list_lock);
    return r;
}

// Check if file is in list of open files, if not, add it.
// Return index if successful, -1 if failed.
int file_list_add(const char *mc_filename)
{
    pthread_mutex_lock(&file_list_lock);

    // If already in list, return that.
    int r = file_list_check_nolock(mc_filename);
    if (r >= 0)
    {
        pthread_mutex_unlock(&file_list_lock);
        return r;
    }

    // Need to add to list.
    for (int i = 0; i < MAX_OPEN_FILES; i++)
    {
        // Find available entry.
        struct open_file_entry *s = &(files_list[i]);
        if (s->is_active)
            continue;

        // Found entry that's available.
        strcpy(s->mc_filename, mc_filename); // Earlier validated that mc_filename is not too long.
        s->is_active = true;
        s->is_init = false;
        pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        s->lock = lock;
        pthread_mutex_lock(&(s->lock));

        // Return entry for file.
        pthread_mutex_unlock(&file_list_lock);
        return i;
    }

    // Failed since no available entries.
    pthread_mutex_unlock(&file_list_lock);
    return -1;
}

// Get number that should be used for new temporary file.
uint64_t get_temp_file_num()
{
    static int cur_num = 0;
    pthread_mutex_lock(&file_list_lock);
    cur_num += 1;
    pthread_mutex_unlock(&file_list_lock);
    return cur_num;
}

// Initialize specified file, option for copying from source or create new.
int init_file(int entry, bool create_new)
{
    // File entry to initialize.
    struct open_file_entry *s = &(files_list[entry]);

    // Get name of temporary file to use.
    char *temp_filename = s->local_filename;
    sprintf(temp_filename, TEMP_PATH_FILES_PREFIX "%" PRIu64 ".temp", get_temp_file_num());

    if (create_new)
    {
        // Create brand new file, no copy from source.
        s->file = fopen(temp_filename, "w+b");
    }
    else
    {
        // Need to copy from source.
        // TODO implement
    }

    // Do rest of initialization.
    s->is_init = true;
    s->need_copy_end = create_new; // can set this later, for append and mixed read/write

    // Done initialization.
    pthread_mutex_unlock(&(files_list[entry].lock));
    return 0;
}
