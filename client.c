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


// A simple client program that uses the key-value service

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "defs.h"
#include "md5.h"
#include "util.h"


// Program arguments

// Host name and port number of the metadata server
static char mserver_host_name[HOST_NAME_MAX] = "";
static uint16_t mserver_port = 0;

// Operations file to be executed; if not specified, the client interactively reads input from stdin
static char ops_file_name[PATH_MAX] = "";
// Log file name
static char log_file_name[PATH_MAX] = "";

static void usage(char **argv)
{
	printf("usage: %s -h <mserver host name> -p <mserver port> [-f <operations file> -l <log file>]\n", argv[0]);
	printf("If the operations file (-f) is not specified, the input is read from stdin\n");
	printf("If the log file (-l) is not specified, log output is written to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
	char option;
	while ((option = getopt(argc, argv, "h:p:f:l:")) != -1) {
		switch(option) {
			case 'h': strncpy(mserver_host_name, optarg, HOST_NAME_MAX); break;
			case 'p': mserver_port = atoi(optarg); break;
			case 'f': strncpy(ops_file_name, optarg, PATH_MAX); break;
			case 'l': strncpy(log_file_name, optarg, PATH_MAX); break;
			default:
				fprintf(stderr, "Invalid option: -%c\n", option);
				return false;
		}
	}

	return (mserver_host_name[0] != '\0') && (mserver_port != 0);
}


// Operation types (in the operation file format)
#define OP_TYPE_NOOP  '0'
#define OP_TYPE_GET   'G'
#define OP_TYPE_PUT   'P'
// CHECK is a special type of operation for checking consistency. The 'value' field is the expected value.
// It is sent as a plain GET operation, and the result is checked against the expected value.
#define OP_TYPE_CHECK 'C'

static op_type get_op_type(char type)
{
	switch (type) {
		case OP_TYPE_NOOP : return OP_NOOP;
		case OP_TYPE_GET  :
		case OP_TYPE_CHECK: return OP_GET;
		case OP_TYPE_PUT  : return OP_PUT;
		default           : return -1;
	}
}

// Maximum length of a string describing an operation
#define MAX_STR_LEN 2048

// Keys and values used by this client are non-empty null-terminated strings
// " " is a special value for CHECK operations meaning that the key shouldn't exist
typedef struct _operation {
	char key[MAX_STR_LEN];
	char value[MAX_STR_LEN];
	char type;// noop/get/put/check
	int count;
	int index;// corresponds to line number in the operation file
} operation;

typedef struct _result {
	char value[MAX_STR_LEN];
	op_status status;
} result;

// Read a key-value operation from the given string. Returns false if the input doesn't match the format
static bool parse_operation(const char *str, operation *op)
{
	assert(str != NULL);
	assert(op != NULL);

	// Format: {0|G|P|C} "<key>" ["<value>" <count>]
	// See 'man 2 scanf' for the matching string format description
	//
	// NOTE: no need to limit the length of strings being read since both the 'op->key'
	//       and 'op->value' buffers have the same length as the whole input string
	int matches_num = sscanf(str, " %c \"%[^\"]\" \"%[^\"]\" %d", &(op->type), op->key, op->value, &(op->count));
	if (matches_num < 1) {// no type
		return false;
	}

	if (matches_num < 4) {// count not specified
		op->count = 1;
	}
	if (op->count <= 0) {
		return false;
	}

	// " " is a special value; set it to an empty string for simplicity
	// Also need to ignore the value for NOOP and GET
	if ((strcmp(op->value, " ") == 0) || (op->type == OP_TYPE_NOOP) || (op->type == OP_TYPE_GET)) {
		op->value[0] = '\0';
	}

	switch (op->type) {
		case OP_TYPE_NOOP :
		case OP_TYPE_GET  : return  matches_num >= 2;// no value needed (unless need to specify count)
		case OP_TYPE_CHECK: return  matches_num >= 3;
		case OP_TYPE_PUT  : return (matches_num >= 3) && (op->value[0] != '\0');// value must be non-empty
		default           : return false;
	}
}


// Contact metadata server and obtain server info for a given key
// Returns a socket fd connected to the server
static int get_key_server(const char key[KEY_SIZE])
{
	assert(key != NULL);

	int mserver_fd = connect_to_server(mserver_host_name, mserver_port);
	if (mserver_fd < 0) {
		return -1;
	}

	locate_request request = {0};
	request.hdr.type = MSG_LOCATE_REQ;
	memcpy(request.key, key, KEY_SIZE);

	char recv_buffer[MAX_MSG_LEN] = {0};
	if (!send_msg(mserver_fd, &request, sizeof(request)) ||
	    !recv_msg(mserver_fd, recv_buffer, sizeof(recv_buffer), MSG_LOCATE_RESP))
	{
		close(mserver_fd);
		return -1;
	}

	close(mserver_fd);// one request per connection
	locate_response *response = (locate_response*)recv_buffer;
	log_write("Key %s is stored on %s:%d\n", key_to_str(key), response->host_name, response->port);

	return connect_to_server(response->host_name, response->port);
}

// Send a GET/PUT operation to the server (connected to via server_fd) and get reply back
// Fills the result with the server's response
// Returns true if the operation was successfully executed (but not necessarily with a SUCCESS status)
// If the server fails to respond, fills the result status with SERVER_FAILURE and returns false
static bool send_operation(int server_fd, const char key[KEY_SIZE], const operation* op, result* res)
{
	assert(key != NULL);
	assert(op  != NULL);
	assert(res != NULL);

	char send_buffer[MAX_MSG_LEN] = {0};
	operation_request *request = (operation_request*)send_buffer;
	request->hdr.type = MSG_OPERATION_REQ;
	request->type = get_op_type(op->type);
	memcpy(request->key, key, KEY_SIZE);

	// Need to copy the value, only for PUT operations
	int value_sz = 0;
	if (request->type == OP_PUT) {
		value_sz = strlen(op->value) + 1;
		strncpy(request->value, op->value, value_sz);
	}

	char recv_buffer[MAX_MSG_LEN] = {0};
	if (!send_msg(server_fd, request, sizeof(*request) + value_sz) ||
	    !recv_msg(server_fd, recv_buffer, sizeof(recv_buffer), MSG_OPERATION_RESP))
	{
		res->status = SERVER_FAILURE;
		return false;
	}

	operation_response *response = (operation_response*)recv_buffer;
	res->status = response->status;
	value_sz = response->hdr.length - sizeof(operation_response);
	strncpy(res->value, response->value, value_sz);

	// A key-value server can return the SERVER_FAILURE status even if it's alive
	// (e.g. if it fails to forward a PUT operation to its secondary replica)
	return res->status != SERVER_FAILURE;
}

// Contact the metadata server, contact the key-value server, get response
static bool execute_operation(const operation *op, result *res)
{
	assert(op != NULL);

	char *key = (char*)md5sum((const unsigned char*)op->key, 0);
	log_write("\"%s\" -> %s\n", op->key, key_to_str(key));

	int server_fd = get_key_server(key);
	if (server_fd < 0) {
		free(key);
		res->status = SERVER_FAILURE;
		return false;
	}

	bool result = send_operation(server_fd, key, op, res);
	free(key);
	close(server_fd);// one request per connection
	return result;
}

// Time (in seconds) between reconnection attempts
static const int retry_interval = 1;

// If the key-value server times out or fails, retry the metadata server
static bool execute_operation_retry(const operation *op, result *res, int attempts)
{
	for (int i = 0; i < attempts; i++) {
		if (execute_operation(op, res)) {
			return true;
		}
		if (i < attempts - 1) {
			log_write("Failed to execute operation #%d, retrying ...\n", op->index);
			sleep(retry_interval);
		}
	}

	res->status = SERVER_FAILURE;
	return false;
}

static void report_operation_failure(int index, op_status status)
{
	assert(status < OP_STATUS_MAX);
	log_error("Operation #%d failed with: %s\n", index, op_status_str[status]);
}

// Validate and output the result of an operation
static bool check_operation_result(const operation *op, const result *res, bool interactive)
{
	assert(op != NULL);
	assert(res != NULL);

	// Check if the operation succeeded (except for the special case of CHECK with no value)
	if ((res->status != SUCCESS) && !((op->type == OP_TYPE_CHECK) && (op->value[0] == '\0'))) {
		report_operation_failure(op->index, res->status);
		return false;
	}

	switch (op->type) {
		case OP_TYPE_NOOP: return true;

		case OP_TYPE_GET:
			if (interactive) printf("value[\"%s\"] == \"%s\"\n", op->key, res->value);
			log_write("value[\"%s\"] == \"%s\"\n", op->key, res->value);
			return true;

		case OP_TYPE_PUT:
			if (interactive) printf("value[\"%s\"] <- \"%s\"\n", op->key, op->value);
			log_write("value[\"%s\"] <- \"%s\"\n", op->key, op->value);
			return true;

		case OP_TYPE_CHECK:
			if (op->value[0] == '\0') {
				// Expect KEY_NOT_FOUND as the result
				if (res->status == KEY_NOT_FOUND) {
					if (interactive) printf("Check #%d passed: key \"%s\" not found\n", op->index, op->key);
					log_write("Check #%d passed: key \"%s\" not found\n", op->index, op->key);
					return true;
				}
				if (res->status == SUCCESS) {
					log_error("Check #%d failed: value[\"%s\"] == \"%s\" != none\n", op->index, op->key, res->value);
					return false;
				}
				report_operation_failure(op->index, res->status);
				return false;
			}

			if (strcmp(op->value, res->value) == 0) {
				if (interactive) printf("Check #%d passed: value[\"%s\"] == \"%s\"\n", op->index, op->key, res->value);
				log_write("Check #%d passed: value[\"%s\"] == \"%s\"\n", op->index, op->key, res->value);
				return true;
			}
			log_error("Check #%d failed: value[\"%s\"] == \"%s\" != \"%s\"\n", op->index, op->key, res->value, op->value);
			return false;

		default:// impossible
			assert(false);
			return false;
	}
}


static void prompt(FILE *input)
{
	if (input == stdin) {
		printf("> ");
		fflush(stdout);
	}
}

// The number of attempts to execute the operation before giving up
static const int max_attempts = 10;

// The number of failed operations before stopping the client
static const int max_failed_ops = 2;

// Read and execute a set of operations from given input stream; returns true if no failures occured
static bool execute_operations(FILE *input)
{
	assert(input != NULL);
	bool success = true;
	int failed_ops_count = 0;

	int index = 1;
	char line[MAX_STR_LEN] = "";
	prompt(input);
	while (fgets(line, sizeof(line), input) != NULL) {
		// Remove trailing '\n'
		size_t len = strnlen(line, sizeof(line));
		if (line[len - 1] == '\n') {
			line[len - 1] = '\0';
		}

		// Skip empty lines
		if (line[0] == '\0') {
			goto next;
		}

		// Parse next operation
		operation op = {0};
		if (!parse_operation(line, &op)) {
			log_error("Invalid operation format\n");
			goto next;
		}
		op.index = index++;

		// Print operation if not interactive
		if (input != stdin) {
			printf("#%d: %c \"%s\" \"%s\" %d\n", op.index, op.type, op.key, op.value, op.count);
		}

		// Execute the operation (possibly multiple times)
		for (int i = 0; i < op.count; i++) {
			result res = {0};
			if (execute_operation_retry(&op, &res, max_attempts)) {
				if (!check_operation_result(&op, &res, input == stdin)) {
					failed_ops_count++;
					success = false;
				}
			} else {
				assert(res.status == SERVER_FAILURE);
				report_operation_failure(op.index, res.status);
				failed_ops_count++;
				success = false;
			}

			// Stop after failures if not interactive
			if ((input != stdin) && (failed_ops_count >= max_failed_ops)) break;
		}

		// Stop after failures if not interactive
		if ((input != stdin) && (failed_ops_count >= max_failed_ops)) {
			assert(!success);
			log_error("%d operations failed, exiting\n", failed_ops_count);
			break;
		}

	next:
		prompt(input);
	}

	printf("\n");
	return success;
}


int main(int argc, char **argv)
{
	signal(SIGPIPE, SIG_IGN);

	if (!parse_args(argc, argv)) {
		usage(argv);
		return 1;
	}

	open_log(log_file_name);

	bool success = false;
	// If the operation file is not given, read input from stdin
	if (ops_file_name[0] != '\0') {
		FILE *ops_file = fopen(ops_file_name, "r");
		if (ops_file == NULL) {
			perror(ops_file_name);
			return 1;
		}

		log_write("Reading input from %s\n", ops_file_name);
		success = execute_operations(ops_file);

		fclose(ops_file);
	} else {
		success = execute_operations(stdin);
	}

	// Return 0 only if no failures occured
	return success ? 0 : 1;
}
