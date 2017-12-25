// Copyright (C) 2016, 2017 Alexey Khrabrov, Bogdan Simion
//
// Distributed under the terms of the GNU General Public License.
//
// This file is part of Assignment 3, CSC469, Fall 2017.
//
// This is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This file is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this file.  If not, see <http://www.gnu.org/licenses/>.


// Various helper functions used by the programs (client, mserver and server)

#ifndef _UTIL_H_
#define _UTIL_H_

#include <stdbool.h>

#include <sys/types.h>

#include "defs.h"


// Logging functions

// If the log file can't be opened, the log output is directed to stdout
void open_log(const char *file_name);

// Write a message to the log file
// Accepts a variable-length list of arguments (like printf)
void log_write(const char *format, ...);

// Flush buffered log output to the file
void log_flush();

// Write a message to both the log file and stderr
#define log_error(...)            \
({                                \
	fflush(stdout);               \
	fprintf(stderr, __VA_ARGS__); \
	log_write(__VA_ARGS__);       \
	log_flush();                  \
})

// perror()-style function for writing errors to both stderr and the log file
// Prefixes messages with the pid of the calling process, so that you can distinguish between messages from mserver and
// servers; pids of the server processes are available in mserver.c in the server_node structs
void log_perror(const char *function);

// Convert a key to its string represenation; the resulting string is allocated on stack
#define key_to_str(key)                         \
({                                              \
	char _str[KEY_SIZE * 2 + 1] = "";           \
	key_to_str_buffer(key, _str, sizeof(_str)); \
})

char *key_to_str_buffer(const char key[KEY_SIZE], char *buffer, size_t length);

// Get current time string in the 'ctime' format (but without the trailing '\n')
char *current_time_str();


// TCP client functions

// Connect to a TCP server given its host name and port number; returns a connected socket fd
int connect_to_server(const char *host_name, uint16_t port);

// Read the whole "packet" from a TCP socket; returns the number of bytes read (or -1 on failure)
// Doesn't stop reading until either the buffer is full, an EOF is encountered, or an error occurs
ssize_t read_whole(int fd, void *buffer, size_t length);

// Write message contents to log, based on its type
// The 'received' argument must be true if this message was received current program
void log_msg(const void *msg, bool received);

// Write a message to a TCP socket
// Returns true on success. Takes care of the byte order and validates the message
// Note that this function modifies message contents, so e.g. it cannot be re-send again using this function
bool send_msg(int fd, void *buffer, size_t length);

// Read a single message from TCP socket
// Returns true on success. Takes care of the byte order and validates the message
// expected_type == -1 means any type
bool recv_msg(int fd, void *buffer, size_t length, msg_type expected_type);


// TCP server functions

// Useful for maximum fd computation for select() calls
#define max(a, b)        \
({                       \
	typeof(a) _a = (a);  \
	typeof(b) _b = (b);  \
	(_a > _b) ? _a : _b; \
})

// If fd is valid (!= -1), closes it, sets it to -1, and returns true; otherwise, returns false
bool close_safe(int *fd);

// Get the host name that server is running on
int get_local_host_name(char *str, size_t length);

// Start a TCP server on a given port; returns listening socket fd
// If port == 0, bind to an arbitrary port assigned by the OS; new_port must be != NULL in this case
// If port != 0 and bind fails, then an arbitray port is chosen (and returned via *new_port) if new_port != NULL
int create_server(uint16_t port, int max_sessions, uint16_t *new_port);

// Accept an incoming TCP connection; returns index in the fd table
int accept_connection(int fd, int *fd_table, int fd_table_size);

// Returns a string with a timestamp, the hostname and the port number of the peer connected to the socket fd
// The port number is converted from network byte order to host byte order before printing it into the string
int get_peer_info(int fd, char *str, size_t length);


// Process management functions

// Wait for a child process to terminate, with a timeout (in seconds)
pid_t waitpid_timeout(pid_t pid, int *status, int timeout);

// Wait for a child process to terminate, killing it if the timeout (in seconds) expires
// Returns false if had to kill the process
bool wait_or_kill(pid_t pid, int timeout);

// If pid is valid (> 0), waits for it to terminate with a timeout (in seconds) and kills it if the timeout expires,
// sets it to 0, and returns true; otherwise, returns false
bool kill_safe(pid_t *pid, int timeout);


// Key space partitioning
//
// Each server gets a unique identifier sid that falls into [0, num_servers - 1]
// Each server is responsible for the set of keys k that meet: k % num_servers == sid
// Each server is thus passed the sid and num_servers when spawned, to know which set of keys it is responsible for.
//
// We also make the simplifying assumption that if the primary replica for a key is on server i,
// then the secondary replica is located on server (i+1) % num_servers.
// For simplicity, we can assume that this replication policy is known by all servers by convention.

// Get primary key server id for a key
int key_server_id(const char key[KEY_SIZE], int num_servers);

// Get secondary server id for given primary server id
int secondary_server_id(int server_id, int num_servers);

// Get primary server id for given secondary server id
int primary_server_id(int server_id, int num_servers);


#endif// _UTIL_H_
