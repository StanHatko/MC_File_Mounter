/**
 * Handle individual file system requests and send to MinIO mc client.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

// Libraries
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

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

// Function that reads input from file.
int file_read(std::string temp_path_base)
{
    return -2;
}

// Function that writes output to file.
int file_write(std::string temp_path_base)
{
    return -3;
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
    std::printf("Truncate to size %zu.\n", new_size);
    int r = truncate(cache_path.c_str(), new_size);
    std::cout << "Truncate has return value: " << r << std::endl;
    return r;
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
