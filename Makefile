# Build the MinIO-mc mounting program and dependencies.

build:
	gcc request_handler.c -o request_handler -Wall
	gcc file_sys.c -o file_sys -Wall `pkg-config fuse --cflags --libs`
	echo 'To Mount: ./file_sys -f [mount point]'

clean:
	rm -f file_sys request_handler
