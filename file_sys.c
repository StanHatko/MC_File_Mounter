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
	log_path("to access", path);
	// TODO: implement null version of this?
	return -1;
}

// FUSE operation: chmod
static int do_chmod(const char *path, mode_t mode)
{
	log_operation("chmod");
	log_path("to chmod", path);
	// No-op implementation.
	return 0;
}

// FUSE operation: flush
static int do_flush(const char *path, struct fuse_file_info *info)
{
	log_operation("flush");
	log_path("to flush", path);
	char temp_path_base[TEMP_PATH_BUF_BASE_SIZE];
	get_temp_file_base(temp_path_base);

	// Send parameters what to write.
	WRITE_OP_INPUT("path", path, strlen(path));

	// Invoke the main handler program.
	int r = invoke_handler("flush", temp_path_base);

	return r;
}

// FUSE operation: getattr
static int do_getattr(const char *path, struct stat *st)
{
	log_operation("getattr");
	log_path("to get attributes", path);
	// TODO: REPLACE with get real info from MinIO

	st->st_uid = getuid(); // The owner of the file/directory is the user who mounted the filesystem
	st->st_gid = getgid(); // The group of the file/directory is the same as the group of the user who mounted the filesystem

	st->st_atime = time(NULL); // The last "a"ccess of the file/directory is right now
	st->st_mtime = time(NULL); // The last "m"odification of the file/directory is right now

	if (strcmp(path, "/") == 0) // TODO: ADD support for subdirectories
	{
		st->st_mode = 040777;
		st->st_nlink = 2; // Why "two" hardlinks instead of "one"? The answer is here: http://unix.stackexchange.com/a/101536
	}
	else
	{
		st->st_mode = 0100777;
		st->st_nlink = 1;
		st->st_size = 1024;
	}

	return 0;

#if 0
	if (strcmp(path, "/") == 0 || is_dir(path) == 1)
	{
		st->st_mode = 0100777;
		st->st_nlink = 2; // Why "two" hardlinks instead of "one"? The answer is here: http://unix.stackexchange.com/a/101536
	}
	else if (is_file(path) == 1)
	{
		st->st_mode = 040777;
		st->st_nlink = 1;
		st->st_size = 1024;
	}
	else
	{
		return -ENOENT;
	}
#endif
}

// FUSE operation: mkdir
static int do_mkdir(const char *path, mode_t mode)
{
	log_operation("mkdir");
	log_path("directory to create", path);
	// TODO: REPLACE
	return -1;
}

// FUSE operation: mknod
static int do_mknod(const char *path, mode_t mode, dev_t rdev)
{
	log_operation("mknod");
	log_path("to create", path);
	// TODO: IMPLEMENT
	return -1;
#if 0
	// Sample implementation.
	path++;
	add_file(path);

	return 0;
#endif
}

// FUSE operation: read
static int do_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
	log_operation("read");
	log_path("to read", path);
	char temp_path_base[TEMP_PATH_BUF_BASE_SIZE];
	get_temp_file_base(temp_path_base);

	// Send parameters what to read.
	WRITE_OP_INPUT("size", &size, sizeof(size_t));
	WRITE_OP_INPUT("offset", &offset, sizeof(size_t));
	WRITE_OP_INPUT("path", path, strlen(path));

	// Invoke the main handler program.
	int r = invoke_handler("read", temp_path_base);
	if (r != 0)
		return -1; // TODO adjust

	// Get response (exact contents to read in temp file) and save to buffer.
	char temp_path_out[TEMP_PATH_BUF_FULL_SIZE];
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
	log_path("list contents", path);
	// TODO: IMPLEMENT PROPERLY

	filler(buffer, ".", NULL, 0);  // Current Directory
	filler(buffer, "..", NULL, 0); // Parent Directory

	filler(buffer, "/2", NULL, 0); // fixed file, later implement properly

	return 0;

#if 0
	// Sample implementation.

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
#endif
}

// FUSE operation: release
static int do_release(const char *path, struct fuse_file_info *info)
{
	log_operation("release");
	log_path("to close file", path);
	// TODO: implement
	return 0;
}

// FUSE operation: rename
static int do_rename(const char *source_path, const char *dest_path)
{
	log_operation("rename");
	log_path("source file", source_path);
	log_path("destination file", dest_path);
	// TODO: implement
	// Not real rename with mc commands!
	return -1;
}

// FUSE operation: rmdir
static int do_rmdir(const char *path)
{
	log_operation("rmdir");
	log_path("directory to remove", path);
	// TODO: implement
	return -1;
}

// FUSE operation: truncate
static int do_truncate(const char *path, off_t new_size)
{
	log_operation("truncate");
	log_path("to truncate", path);
	char temp_path_base[TEMP_PATH_BUF_BASE_SIZE];
	get_temp_file_base(temp_path_base);

	// Send parameters what to resize to.
	WRITE_OP_INPUT("path", path, strlen(path));
	WRITE_OP_INPUT("size", &new_size, sizeof(off_t));

	// Invoke the main handler program.
	int r = invoke_handler("truncate", temp_path_base);
	if (r != 0)
		return -1; // TODO adjust

	return 0;
}

// FUSE operation: unlink
static int do_unlink(const char *path)
{
	log_operation("unlink");
	log_path("file to remove", path);
	// TODO: implement
	return -1;
}

// FUSE operation: write
static int do_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *info)
{
	log_operation("write");
	log_path("to write", path);
	char temp_path_base[TEMP_PATH_BUF_BASE_SIZE];
	get_temp_file_base(temp_path_base);

	// Send parameters what to write.
	WRITE_OP_INPUT("size", &size, sizeof(size_t));
	WRITE_OP_INPUT("offset", &offset, sizeof(size_t));
	WRITE_OP_INPUT("path", path, strlen(path));
	WRITE_OP_INPUT("buffer", buffer, size);

	// Invoke the main handler program.
	int r = invoke_handler("write", temp_path_base);
	if (r != 0)
		return -1; // TODO adjust

	// Always writes size bytes, if did not then failure indicated earlier.
	return size;
}

// Structure with functions for necessary operations.
static struct fuse_operations operations = {
	.access = do_access,
	.chmod = do_chmod,
	.flush = do_flush,
	.getattr = do_getattr,
	.mkdir = do_mkdir,
	.mknod = do_mknod,
	.read = do_read,
	.readdir = do_readdir,
	.rename = do_rename,
	.release = do_release,
	.rmdir = do_rmdir,
	.truncate = do_truncate,
	.unlink = do_unlink,
	.write = do_write,
};

// Main run function.
int main(int argc, char *argv[])
{
	init_config();
	return fuse_main(argc, argv, &operations, NULL);
}
