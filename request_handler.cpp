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
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "config.h"

// Function that gets path to MinIO client binary.
std::string get_mc_bin_path()
{
    const char *mc_bin = std::getenv("mc_bin_path");
    if (mc_bin == nullptr)
        throw "Cannot have mc_bin environment variable be missing!";

    std::string s = mc_bin;
    return s;
}

// Function that gets SH256 hex string from temp file.
std::string get_sha256(std::string temp_path_base, std::string extension)
{
    // Invoke command to get SHA256 sum.
    std::string sha_file = temp_path_base + ".sha256";
    std::string full_file = temp_path_base + extension;
    std::cout << "Get SHA256 sum of file: " << full_file << std::endl;

    std::string cmd = "sha256sum " + full_file + " >" + sha_file;
    std::system(cmd.c_str());

    // Read in and return the checksum.
    char sha_sum[65];
    std::memset(sha_sum, 0, 65);

    std::ifstream rf(temp_path_base + ".sha256");
    rf.read(sha_sum, 64);

    std::string sha_str = sha_sum;
    std::cout << "File has SHA256 sum: " << sha_str << std::endl;
    return sha_str;
}

// Function that gets cache file path.
std::string get_cache_path(std::string sha)
{
    const char *prefix = std::getenv("temp_files_prefix");
    if (prefix == nullptr)
        throw "Cannot have temp_files_prefix be NULL.";

    std::string path = prefix;
    path += "_cache_";
    path += sha;
    path += ".bin";
    std::cout << "Using cache file path: " << path << std::endl;
    return path;
}

// Function that escapes string inside file for use in bash.
std::string get_bash_escaped_string(std::string temp_file_base, std::string ext_input, std::string ext_temp)
{
    std::string in_file = temp_file_base + ext_input;
    std::string out_file = temp_file_base + ext_temp;

    // Use jq command to bash-escape string.
    std::string cmd = "jq -r '@sh' >" + out_file + " <" + in_file;
    std::cout << "Escape string in file for bash with command: " << cmd << std::endl;
    int r = system(cmd.c_str());
    if (r != 0)
    {
        std::cout << "Attempt to escape string failed!\n";
        return std::string("");
    }

    // Get the string from the file.
    std::ifstream get_output(out_file);
    char sr[MAX_PATH_LEN];
    get_output.getline(sr, MAX_PATH_LEN - 1);
    return std::string(sr);
}

// Function that reads input from file.
int file_read(std::string temp_path_base)
{
    // Get path of cache file.
    std::string cache_path = get_cache_path(get_sha256(temp_path_base, ".path"));

    // Get metadata for read operation.
    return -1; // TODO implement
}

// Function that writes output to file.
int file_write(std::string temp_path_base)
{
    // Get path of cache file.
    std::string cache_path = get_cache_path(get_sha256(temp_path_base, ".path"));

    // Get offset for write operation.
    std::ifstream file_with_offset(temp_path_base + ".offset", std::ios::binary);
    std::size_t offset;
    file_with_offset >> offset;
    std::printf("Using offset %zu.\n", offset);

    // Get buffer contents and size for write operation.
    std::string path_with_buffer = temp_path_base + ".buffer";
    std::intmax_t nc = std::filesystem::file_size(path_with_buffer);
    std::cout << "Size of buffer to write: " << nc << std::endl;

    char *buf = new char[nc];
    std::ifstream file_with_buffer(path_with_buffer, std::ios::binary);
    file_with_buffer.read(buf, nc);

    if (file_with_buffer.gcount() < nc)
    {
        std::cout << "Failed to read all " << nc << " bytes required!\n";
        return -EIO;
    }

    // Do the write operation on cache file.
    FILE *f = fopen(cache_path.c_str(), "ab");
    if (f == nullptr)
    {
        std::cout << "Failed to open cache file!\n";
        delete[] buf;
        return -EIO;
    }

    fseek(f, offset, SEEK_SET);
    fwrite(buf, 1, nc, f);
    delete[] buf;

    if (ferror(f) != 0)
    {
        std::cout << "Failed I/O on cache file!\n";
        fclose(f);
        return -EIO;
    }

    if (fclose(f) != 0)
    {
        std::cout << "Failed close operation on cache file!\n";
        return -EIO;
    }

    // If reached here, successful.
    return 0;
}

// Function that flushes output file.
int file_flush(std::string temp_path_base)
{
    // TODO IMPLEMENT
    // For now no-op, once MinIO support added will copy cache file to MinIO.
    return 0;
}

// Function that truncates file to specified size.
int file_truncate(std::string temp_path_base)
{
    // Get path of cache file.
    std::string cache_path = get_cache_path(get_sha256(temp_path_base, ".path"));

    // Get correct size.
    std::ifstream file_with_size(temp_path_base + ".size", std::ios::binary);
    std::size_t new_size;
    file_with_size >> new_size;

    // Do the truncate operation.
    std::cout << "Truncate path: " << cache_path << std::endl;
    if (!std::filesystem::exists(cache_path))
    {
        std::cout << "Path does not currently exist, create it.\n";
        std::ofstream(cache_path, std::ios::binary).close();
    }

    std::printf("Truncate to size %zu.\n", new_size);
    int r = truncate(cache_path.c_str(), new_size);
    std::cout << "Truncate has return value: " << r << std::endl;
    return r;
}

// Function that lists files and subdirectories in directory.
int dir_list(std::string temp_path_base)
{
    // Get escaped path to directory with contents.
    std::string escaped_dir_path = get_bash_escaped_string(temp_path_base, ".path", ".escaped");
    std::cout << "List contents of escaped directory path: " << escaped_dir_path << std::endl;

    // List contents of escaped directory.
    std::string contents_path = temp_path_base + ".raw_dir_list";
    std::string cmd_list = get_mc_bin_path() + " ls --json " + escaped_dir_path + " >" + contents_path;
    std::cout << "List using command: " << cmd_list << std::endl;

    int r_list = system(cmd_list.c_str());
    if (r_list != 0)
    {
        std::cout << "Failed to list directory contents, exit.\n";
        return -EIO;
    }

    // Escape the listed directories.
    std::string out_path = temp_path_base + ".out";
    std::string cmd_parse = "jq -r '.key' >" + out_path + " <" + contents_path;
    std::cout << "Parse using command: " << cmd_parse << std::endl;

    int r_escape = system(cmd_parse.c_str());
    if (r_escape != 0)
    {
        std::cout << "Failed to escape listed directory contents, exit.\n";
        return -EIO;
    }

    // The .out file now contains the list of files and directories.
    std::cout << "Successfully listed the directory contents!\n";
    return 0;
}

// Function that handles each incoming request.
int handle_request(std::string request_type, std::string temp_path_base)
{
    std::cout << "Handle request of type: " << request_type << std::endl;
    std::cout << "Request has temporary path base: " << temp_path_base << std::endl;

    // Determine which kind of request to service.
    if (request_type == "read")
    {
        return file_read(temp_path_base);
    }
    else if (request_type == "write")
    {
        return file_write(temp_path_base);
    }
    else if (request_type == "truncate")
    {
        return file_truncate(temp_path_base);
    }
    else if (request_type == "flush")
    {
        return file_flush(temp_path_base);
    }
    else if (request_type == "dir_list")
    {
        return dir_list(temp_path_base);
    }
    else
    {
        std::cout << "No such request kind";
        return -EINVAL;
    }
}

// Main function, called when program is invoked.
int main(int argc, char **argv)
{
    std::ios_base::sync_with_stdio(true);
    if (argc != 3)
    {
        std::cout << "Program request_handler must have two arguments!\n";
        return -EINVAL;
    }

    std::string request_type = argv[1];
    std::string temp_path_base = argv[2];
    return handle_request(request_type, temp_path_base);
}
