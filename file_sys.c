/**
 * Mount file system that interfaces to potentially multiple Python fsspec-based backends.
 * For now still mostly sample code, need to implement methods.
 * By Stan Hatko
 *
 * Based on repo https://github.com/MaaSTaaR/LSYSFS by
 * Mohammed Q. Hussain - http://www.maastaar.net
 *
 * License: GNU GPL
 */

#define FUSE_USE_VERSION 35

#include <errno.h>
#include <fuse.h>
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

// Maximum path length to domain socket file: must later fit into 108 char buffer.
#define BUF_SIZE_DOMAIN_SOCKET 96

// Name of domain socket file.
char domain_socket_file[BUF_SIZE_DOMAIN_SOCKET];

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
	const char *ds = get_config_var("domain_socket_file", BUF_SIZE_DOMAIN_SOCKET);
	strcpy(domain_socket_file, ds);
}

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
	strcpy(addr.sun_path, domain_socket_file); // source limited by BUF_SIZE_DOMAIN_SOCKET

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

#define OPEN_DOMAIN_SOCKET_CHECK_ERROR()            \
	{                                               \
		if (fd < 0)                                 \
		{                                           \
			perror("Could not open domain socket"); \
			return -1; /* TODO adjust?*/            \
		}                                           \
	}

#define SEND_WITH_CHECK_ERROR(var_send, len_send)          \
	{                                                      \
		int send_retval = send(fd, var_send, len_send, 0); \
		if (send_retval < 0)                               \
		{                                                  \
			perror("Domain socket send failed");           \
			close(fd);                                     \
			return send_retval;                            \
		}                                                  \
	}

// FUSE operation: access (check if file exists)
static int do_access(const char *path, int perms)
{
	log_operation("access");
	log_path("to access", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'A';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
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

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'F';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
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
	log_path("to mkdir", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'M';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
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

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'R';
	send(fd, &cmd, 1, 0); // TODO check error

	send(fd, path, strlen(path) + 1, 0); // TODO check error

	send(fd, &size, sizeof(size), 0); // TODO check error

	send(fd, &offset, sizeof(offset), 0); // TODO check error

	int actual_read;
	int recv_retval = recv(fd, &actual_read, sizeof(actual_read), 0);
	if (recv_retval < 0)
	{
		perror('recv failed');
		close(fd);
		return recv_retval;
	}
	if (actual_read < 0)
	{
		perror('Read failed');
		close(fd);
		return -1; // TODO adjust
	}

	int recv_retval_2 = recv(fd, buffer, actual_read, 0);

	close(fd);
	return actual_read;
}

// FUSE operation: readdir
static int do_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	log_operation("readdir");
	log_path("list contents", path);
	char temp_path_base[TEMP_PATH_BUF_BASE_SIZE];
	get_temp_file_base(temp_path_base);

	filler(buffer, ".", NULL, 0);  // Current Directory
	filler(buffer, "..", NULL, 0); // Parent Directory

	// Adjust path for MinIO mc ls operation.
	char minio_path[4 * MAX_PATH_LEN];
	sprintf(minio_path, "\"%s%s\"", mc_data_prefix, path);

	// Send parameters of directory to list.
	WRITE_OP_INPUT("path", minio_path, strlen(minio_path));

	// Send request and wait for response to become available.
	CREATE_START_REQUEST();
	wait_for_output(temp_path_base);

	// Get list of files in the directory.
	char temp_path_out[TEMP_PATH_BUF_FULL_SIZE];
	sprintf(temp_path_out, "%s.out", temp_path_base);

	FILE *fr = fopen(temp_path_out, "r");
	if (fr == NULL)
		return -1; // TODO adjust

	while (true)
	{
		char s[MAX_PATH_LEN];
		fgets(s, MAX_PATH_LEN - 1, fr);

		if (ferror(fr))
		{
			fclose(fr);
			return -1; // TODO adjust
		}

		// If end of file occurred, done.
		if (feof(fr))
		{
			fclose(fr);
			break;
		}

		// Remove trailing newline, by replacing with null character.
		char *ch = s;
		while (*ch != 0)
		{
			if (*ch == '\n')
				*ch = 0;
			ch++;
		}

		// Add file or directory to list.
		filler(buffer, s, NULL, 0);
	}

	return 0;
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

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'D';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
}

// FUSE operation: truncate
static int do_truncate(const char *path, off_t new_size)
{
	log_operation("truncate");
	log_path("to truncate", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'T';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&new_size, sizeof(new_size));

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
}

// FUSE operation: unlink
static int do_unlink(const char *path)
{
	log_operation("unlink");
	log_path("file to remove", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'U';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
}

// FUSE operation: write
static int do_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *info)
{
	log_operation("write");
	log_path("to write", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'W';
	send(fd, &cmd, 1, 0); // TODO check error

	send(fd, path, strlen(path) + 1, 0); // TODO check error

	send(fd, &size, sizeof(size), 0); // TODO check error

	send(fd, &offset, sizeof(offset), 0); // TODO check error

	send(fd, buffer, size, 0); // TODO check error

	int retval;
	recv(fd, &retval, sizeof(retval), 0); // TODO check error

	close(fd);
	return retval;
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
