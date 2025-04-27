/**
 * Internal support functions for FUSE mounting program.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "config.h"

// Name of domain socket file.
const char *domain_socket_file;

// Connect to domain socket.
int open_domain_socket()
{
    // Create the socket.
    int fd = socket(PF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("Socket connection failed");
        return -1;
    }

    // Create address in format usable by socket connect.
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, domain_socket_file); // check strlen is at most 107 characters

    // Connect to the domain socket file.
    int conn_status = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (conn_status < 0)
    {
        perror("Connect failed");
        return -1;
    }

    // Return the socket obtained.
    return fd;
}

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
    domain_socket_file = get_config_var("domain_socket_file", 100);
}

// Function that waits for output to become available.
void wait_for_output(const char *temp_path_base)
{
    char path[TEMP_PATH_BUF_FULL_SIZE];
    sprintf(path, "%s.done", temp_path_base);

    while (true)
    {
        if (access(path, F_OK) == 0)
        {
            // Done file exists, exit loop.
            return;
        }

        // Wait a bit before checking again.
        struct timespec tim, tim2;
        tim.tv_sec = 0;
        tim.tv_nsec = TIME_SLEEP_NANOSEC;
        nanosleep(&tim, &tim2);
    }
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
        if (f == NULL)                                              \
            return -1; /*TODO adjust*/                              \
        fclose(f);                                                  \
        if (f == NULL)                                              \
            return -1; /*TODO adjust*/                              \
    }

// Macro that creates ".start" file to indicate to backend server to process request.
#define CREATE_START_REQUEST()                     \
    {                                              \
        char path[TEMP_PATH_BUF_FULL_SIZE];        \
        sprintf(path, "%s.start", temp_path_base); \
        FILE *f = fopen(path, "wb");               \
        if (f == NULL)                             \
            return -1; /*TODO adjust*/             \
        fclose(f);                                 \
    }
