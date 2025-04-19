# Build the MinIO-mc mounting program and dependencies.

build:
	gcc file_list.c -o file_list.o -c -Wall
	gcc request_handler.c -o request_handler -Wall
	gcc mc_file_sys.c -o mc_file_sys -Wall `pkg-config fuse --cflags --libs`
	echo 'To Mount: ./mc_file_sys -f [mount point]'

clean:
	rm -f mc_file_sys request_handler file_list.o
