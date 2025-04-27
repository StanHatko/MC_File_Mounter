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

#define RECV_WITH_CHECK_ERROR(var_recv, len_recv)          \
	{                                                      \
		int recv_retval = recv(fd, var_recv, len_recv, 0); \
		if (recv_retval < 0)                               \
		{                                                  \
			perror("Domain socket recv failed");           \
			close(fd);                                     \
			return recv_retval;                            \
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
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: chmod
static int do_chmod(const char *path, mode_t mode)
{
	log_operation("chmod");
	log_path("to chmod", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'M';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&mode, sizeof(mode_t));

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: chown
static int do_chown(const char *path, uid_t uid, gid_t gid)
{
	log_operation("chown");
	log_path("to chmod", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'I';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&uid, sizeof(uid_t));
	SEND_WITH_CHECK_ERROR(&gid, sizeof(gid_t));

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: create
static int do_create(const char *path, mode_t mode, dev_t rdev)
{
	log_operation("create");
	log_path("to create", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'C';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&mode, sizeof(mode_t));

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
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
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: getattr
static int do_getattr(const char *path, struct stat *st)
{
	log_operation("getattr");
	log_path("to get attributes", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'G';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	if (retval < 0)
	{
		perror("Underlying getattr failed");
		close(fd);
		return retval;
	}

	RECV_WITH_CHECK_ERROR(&st->st_uid, sizeof(st->st_uid)); // owner
	RECV_WITH_CHECK_ERROR(&st->st_gid, sizeof(st->st_gid)); // group of owner

	RECV_WITH_CHECK_ERROR(&st->st_atime, sizeof(st->st_atime)); // access time
	RECV_WITH_CHECK_ERROR(&st->st_mtime, sizeof(st->st_mtime)); // modification time

	RECV_WITH_CHECK_ERROR(&st->st_mode, sizeof(st->st_mode));	// mode of file
	RECV_WITH_CHECK_ERROR(&st->st_nlink, sizeof(st->st_nlink)); // number of links
	RECV_WITH_CHECK_ERROR(&st->st_size, sizeof(st->st_size));	// size (set to 0 for directories)

	close(fd);
	return 0;
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
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: open
static int do_open(const char *path, struct fuse_file_info *info)
{
	log_operation("open");
	log_path("to open", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'O';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: read
static int do_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
	log_operation("read");
	log_path("to read", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'R';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&size, sizeof(size));
	SEND_WITH_CHECK_ERROR(&offset, sizeof(offset));

	int bytes_read;
	RECV_WITH_CHECK_ERROR(&bytes_read, sizeof(bytes_read));
	if (bytes_read < 0)
	{
		perror('Underlying read operation failed');
		close(fd);
		return bytes_read;
	}

	RECV_WITH_CHECK_ERROR(&buffer, bytes_read);
	close(fd);
	return bytes_read;
}

// FUSE operation: readdir (get directory listing)
static int do_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	log_operation("readdir");
	log_path("list contents", path);

	filler(buffer, ".", NULL, 0);  // Current Directory
	filler(buffer, "..", NULL, 0); // Parent Directory

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'L';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&offset, sizeof(offset));

	int num_entries;
	RECV_WITH_CHECK_ERROR(&num_entries, sizeof(num_entries));
	if (num_entries < 0)
	{
		perror('Underlying readdir operation failed');
		close(fd);
		return num_entries;
	}

	for (int i = 0; i < num_entries; i++)
	{
		short path_len;
		RECV_WITH_CHECK_ERROR(&path_len, sizeof(path_len));

		// Calloc initializes so guaranteed to be null-terminated.
		char *entry_path = calloc(path_len + 1, 1);
		if (entry_path == NULL)
		{
			perror('Allocate memory failed');
			return -ENOMEM;
		}

		int recv_status = recv(fd, entry_path, path_len, 0);
		if (recv_status < 0)
		{
			perror("Operation recv to get path failed");
			free(entry_path);
			return recv_status;
		}
		if (recv_status != path_len)
		{
			perror("Operation recv to get path did not get enough bytes");
			free(entry_path);
			return -EIO;
		}

		// Add file or directory to list.
		filler(buffer, entry_path, NULL, 0);
		free(entry_path);
	}

	return 0;
}

// FUSE operation: release
static int do_release(const char *path, struct fuse_file_info *info)
{
	log_operation("release");
	log_path("to close file", path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'X';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// FUSE operation: rename
static int do_rename(const char *source_path, const char *dest_path)
{
	log_operation("rename");
	log_path("source file", source_path);
	log_path("destination file", dest_path);

	int fd = open_domain_socket();
	OPEN_DOMAIN_SOCKET_CHECK_ERROR();

	char cmd = 'N';
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(source_path, strlen(source_path) + 1);
	SEND_WITH_CHECK_ERROR(dest_path, strlen(dest_path) + 1);

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
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
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
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
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
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
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
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
	SEND_WITH_CHECK_ERROR(&cmd, 1);
	SEND_WITH_CHECK_ERROR(path, strlen(path) + 1);
	SEND_WITH_CHECK_ERROR(&size, sizeof(size));
	SEND_WITH_CHECK_ERROR(&offset, sizeof(offset));
	SEND_WITH_CHECK_ERROR(buffer, size);

	int retval;
	RECV_WITH_CHECK_ERROR(&retval, sizeof(retval));
	close(fd);
	return retval;
}

// Structure with functions for necessary operations.
static struct fuse_operations operations = {
	.access = do_access,
	.chmod = do_chmod,
	.chown = do_chown,
	.create = do_create,
	.flush = do_flush,
	.getattr = do_getattr,
	.mkdir = do_mkdir,
	.open = do_open,
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
