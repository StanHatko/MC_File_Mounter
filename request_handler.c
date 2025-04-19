/**
 * Handle individual file system requests and send to MinIO mc client.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

// Libraries
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

// Implement functions to handle requests.

int handle_request(const char *temp_path)
{
    fprintf(stderr, "Load request in temporary path: %s\n", temp_path);

    // Open temp file with input.
    FILE *input = fopen(temp_path, "rb");
    if (input == NULL)
    {
        fputs("Attempt to open input temporary file failed!\n", stderr);
        return -ENOENT;
    }

    // Get command to execute.
    int cmd;
    int input_ret = fread(&cmd, sizeof(int), 1, input);
    if (input_ret != 1)
    {
        fputs("Attempt to get command from temporary file failed!\n", stderr);
        exit(-EPROTO);
    }

    // Execute rest based on command.
    switch (cmd)
    {
    case 1:
        fputs("Create brand new file.\n", stderr);
        // TODO implement
        return -1;
    case 2:
        fputs("Open existing file from MinIO storage.\n", stderr);
        // TODO implement
        return -1;
    case 3:
        fputs("Read from temp file cache.\n", stderr);
        // TODO implement
        return -1;
    case 4:
        fputs("Write to temp file cache.\n", stderr);
        // TODO implement
        return -1;
    case 5:
        fputs("Write out temp file cache to MinIO storage.\n", stderr);
        // TODO implement
        return -1;
    case 6:
        fputs("Close file (flush and free temp cache).\n", stderr);
        // TODO implement
        return -1;
    default:
        fprintf(stderr, "Command not implemented: %d!\n", cmd);
        return -EINVAL;
    }
}

// Main function, called when program is invoked.
int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fputs("Program request_handler must have one argument, the temporary file!\n", stderr);
        exit(-EINVAL);
    }
    return handle_request(argv[1]);
}
