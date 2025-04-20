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

#define FUSE_USE_VERSION 35
#include <fuse.h>

#include "file_sys_support.c"

// FUSE operation: access
static int do_access(const char *path, int perms)
{
	log_operation("access");
	// TODO: implement null version of this?
	return -1;
}

// FUSE operation: chmod
static int do_chmod(const char *path, mode_t mode)
{
	log_operation("chmod");
	// No-op implementation.
	return 0;
}

// FUSE operation: flush
static int do_flush(const char *path, struct fuse_file_info *info)
{
	log_operation("flush");
	// TODO: implement
	return -1;
}

// FUSE operation: getattr
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

// FUSE operation: mkdir
static int do_mkdir(const char *path, mode_t mode)
{
	log_operation("mkdir");
	// Sample implementation.
	// TODO: REPLACE
	path++;
	add_dir(path);

	return 0;
}

// FUSE operation: mknod
static int do_mknod(const char *path, mode_t mode, dev_t rdev)
{
	log_operation("mknod");
	// Sample implementation.
	// TODO: REPLACE
	path++;
	add_file(path);

	return 0;
}

// FUSE operation: read
static int do_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
	log_operation("read");
	char temp_path_base[TEMP_PATH_BUF_SIZE];
	get_temp_file_base(temp_path_base);

	// Send parameters what to read.
	WRITE_OP_INPUT("size", &size, sizeof(size_t));
	WRITE_OP_INPUT("offset", &offset, sizeof(size_t));
	WRITE_OP_INPUT("path", path, strlen(path));

	// Invoke the main handler program.
	invoke_handler("read", temp_path_base);

	// Get response (exact contents to read in temp file) and save to buffer.
	char temp_path_out[TEMP_PATH_BUF_SIZE];
	sprintf(temp_path_out, "%s.out", temp_path_base);

	FILE *fr = fopen(temp_path_out, "rb");
	if (fr == NULL)
		return -1; // TODO adjust

	int bytes_read = fread(buffer, size, 0, fr);
	fclose(fr);

	return bytes_read;
}

// FUSE operation: readdir
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

// FUSE operation: release
static int do_release(const char *path, struct fuse_file_info *info)
{
	log_operation("release");
	// TODO: implement
	do_flush(path, info);
	return -1;
}

// FUSE operation: rename
static int do_rename(const char *source_path, const char *dest_path)
{
	log_operation("rename");
	// TODO: implement
	// Not real rename with mc commands!
	return -1;
}

// FUSE operation: rmdir
static int do_rmdir(const char *path)
{
	log_operation("rmdir");
	// TODO: implement
	return -1;
}

// FUSE operation: unlink
static int do_unlink(const char *path)
{
	log_operation("unlink");
	// TODO: implement
	return -1;
}

// FUSE operation: write
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
	init_config();
	return fuse_main(argc, argv, &operations, NULL);
}
