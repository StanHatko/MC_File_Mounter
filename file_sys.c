/**
 * Mount file system that can be accessed using MinIO mc command.
 * For now still mostly sample code, need to implement methods.
 * By Stan Hatko
 *
 * Based on repo https://github.com/MaaSTaaR/LSYSFS by
 * Mohammed Q. Hussain - http://www.maastaar.net
 *
 * License: GNU GPL
 */

// Libraries: configuration and header files

#define FUSE_USE_VERSION 35

#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

// Program configuration

#define MC_PATH "./mc"

// Global vars

char dir_list[256][256];
int curr_dir_idx = -1;

char files_list[256][256];
int curr_file_idx = -1;

char files_content[256][256];
int curr_file_content_idx = -1;

// Implementations of internal functions.

void log_operation(const char *op_name)
{
	fprintf(stderr, "Perform operation: %s\n", op_name);
}

void add_dir(const char *dir_name)
{
	// Sample implementation.
	// TODO: REPLACE
	curr_dir_idx++;
	strcpy(dir_list[curr_dir_idx], dir_name);
}

int is_dir(const char *path)
{
	// Sample implementation.
	// TODO: REPLACE
	path++; // Eliminating "/" in the path

	for (int curr_idx = 0; curr_idx <= curr_dir_idx; curr_idx++)
		if (strcmp(path, dir_list[curr_idx]) == 0)
			return 1;

	return 0;
}

void add_file(const char *filename)
{
	// Sample implementation.
	// TODO: REPLACE
	curr_file_idx++;
	strcpy(files_list[curr_file_idx], filename);

	curr_file_content_idx++;
	strcpy(files_content[curr_file_content_idx], "");
}

int is_file(const char *path)
{
	// Sample implementation.
	// TODO: REPLACE
	path++; // Eliminating "/" in the path

	for (int curr_idx = 0; curr_idx <= curr_file_idx; curr_idx++)
		if (strcmp(path, files_list[curr_idx]) == 0)
			return 1;

	return 0;
}

int get_file_index(const char *path)
{
	// Sample implementation.
	// TODO: REPLACE
	path++; // Eliminating "/" in the path

	for (int curr_idx = 0; curr_idx <= curr_file_idx; curr_idx++)
		if (strcmp(path, files_list[curr_idx]) == 0)
			return curr_idx;

	return -1;
}

void write_to_file(const char *path, const char *new_content)
{
	// Sample implementation.
	// TODO: REPLACE
	int file_idx = get_file_index(path);

	if (file_idx == -1) // No such file
		return;

	strcpy(files_content[file_idx], new_content);
}

// Implementations of necessary FUSE functions

static int do_access(const char *path, int perms)
{
	log_operation("access");
	// TODO: implement null version of this?
	return -1;
}

static int do_chmod(const char *path, mode_t mode)
{
	log_operation("chmod");
	// No-op implementation.
	return 0;
}

static int do_flush(const char *path, struct fuse_file_info *info)
{
	log_operation("flush");
	// TODO: implement
	return -1;
}

static int do_getattr(const char *path, struct stat *st)
{
	log_operation("getattr");
	// Sample implementation.
	// TODO: REPLACE

	st->st_uid = getuid();	   // The owner of the file/directory is the user who mounted the filesystem
	st->st_gid = getgid();	   // The group of the file/directory is the same as the group of the user who mounted the filesystem
	st->st_atime = time(NULL); // The last "a"ccess of the file/directory is right now
	st->st_mtime = time(NULL); // The last "m"odification of the file/directory is right now

	if (strcmp(path, "/") == 0 || is_dir(path) == 1)
	{
		st->st_mode = S_IFDIR | 0755;
		st->st_nlink = 2; // Why "two" hardlinks instead of "one"? The answer is here: http://unix.stackexchange.com/a/101536
	}
	else if (is_file(path) == 1)
	{
		st->st_mode = S_IFREG | 0644;
		st->st_nlink = 1;
		st->st_size = 1024;
	}
	else
	{
		return -ENOENT;
	}

	return 0;
}

static int do_mkdir(const char *path, mode_t mode)
{
	log_operation("mkdir");
	// Sample implementation.
	// TODO: REPLACE
	path++;
	add_dir(path);

	return 0;
}

static int do_mknod(const char *path, mode_t mode, dev_t rdev)
{
	log_operation("mknod");
	// Sample implementation.
	// TODO: REPLACE
	path++;
	add_file(path);

	return 0;
}

static int do_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
	log_operation("read");
	// Sample implementation.
	// TODO: REPLACE
	int file_idx = get_file_index(path);

	if (file_idx == -1)
		return -1;

	char *content = files_content[file_idx];

	memcpy(buffer, content + offset, size);

	return strlen(content) - offset;
}

static int do_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	log_operation("readdir");
	// Sample implementation.
	// TODO: REPLACE

	filler(buffer, ".", NULL, 0);  // Current Directory
	filler(buffer, "..", NULL, 0); // Parent Directory

	if (strcmp(path, "/") == 0) // If the user is trying to show the files/directories of the root directory show the following
	{
		for (int curr_idx = 0; curr_idx <= curr_dir_idx; curr_idx++)
			filler(buffer, dir_list[curr_idx], NULL, 0);

		for (int curr_idx = 0; curr_idx <= curr_file_idx; curr_idx++)
			filler(buffer, files_list[curr_idx], NULL, 0);
	}

	return 0;
}

static int do_release(const char *path, struct fuse_file_info *info)
{
	log_operation("release");
	// TODO: implement
	do_flush(path, info);
	return -1;
}

static int do_rename(const char *source_path, const char *dest_path)
{
	log_operation("rename");
	// TODO: implement
	// Not real rename with mc commands!
	return -1;
}

static int do_rmdir(const char *path)
{
	log_operation("rmdir");
	// TODO: implement
	return -1;
}

static int do_unlink(const char *path)
{
	log_operation("unlink");
	// TODO: implement
	return -1;
}

static int do_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *info)
{
	log_operation("write");
	// Sample implementation.
	// TODO: REPLACE
	write_to_file(path, buffer);

	return size;
}

// Structure with functions for necessary operations.
static struct fuse_operations operations = {
	.access = do_access,
	.chmod = do_chmod,
	.getattr = do_getattr,
	.mkdir = do_mkdir,
	.mknod = do_mknod,
	.read = do_read,
	.readdir = do_readdir,
	.rename = do_rename,
	.release = do_release,
	.rmdir = do_rmdir,
	.unlink = do_unlink,
	.write = do_write,
};

// Main run function.
int main(int argc, char *argv[])
{
	return fuse_main(argc, argv, &operations, NULL);
}
