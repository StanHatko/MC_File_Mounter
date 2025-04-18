COMPILER = gcc
FILESYSTEM_FILES = mc_file_sys.c

build: $(FILESYSTEM_FILES)
	$(COMPILER) $(FILESYSTEM_FILES) -o mc_file_sys `pkg-config fuse --cflags --libs`
	echo 'To Mount: ./mc_file_sys -f [mount point]'
