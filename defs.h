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


// Definition of the communication protocol

#ifndef _DEFS_H_
#define _DEFS_H_

#include <stdint.h>


// Message types
typedef enum {
	MSG_NONE = 0,

	// Locating key-value server for a given key
	MSG_LOCATE_REQ,
	MSG_LOCATE_RESP,

	// GET/PUT operations
	MSG_OPERATION_REQ,
	MSG_OPERATION_RESP,

	// Control requests (for failure detection and recovery purposes) serviced by the metadata server
	// NOTE: mserver control requests do not require a response
	MSG_MSERVER_CTRL_REQ,

	// Control requests (for failure detection and recovery purposes) serviced by a key-value server
	MSG_SERVER_CTRL_REQ,
	MSG_SERVER_CTRL_RESP,

	MSG_TYPE_MAX,
// "packed" enum means that it has the least possible (hence platform-independent) size, 1 byte in this case
} __attribute__((packed)) msg_type;


// For logging purposes
__attribute__((unused))// to suppress possible "unused variable" warnings
static const char *msg_type_str[MSG_TYPE_MAX] = {
	"NONE",

	"LOCATE request",
	"LOCATE response",

	"OPERATION request",
	"OPERATION response",

	"MSERVER CTRL request",

	"SERVER CTRL request",
	"SERVER CTRL response"
};


// A "magic number" in the message header (for checking consistency)
#define HDR_MAGIC 0x7B

// Maximum length of a message
#define MAX_MSG_LEN 2048

// A common header for all messages
typedef struct _msg_hdr {
	char magic;
	msg_type type;
	uint16_t length;
// "packed" struct means there is no padding between the fields (so that the layout is platform-independent)
} __attribute__((packed)) msg_hdr;


// We use 128-bit (16-byte) MD5 hash as a key
#define KEY_SIZE 16

// "Locate" request: get server address for a particular key

typedef struct _locate_request {
	msg_hdr hdr;
	char key[KEY_SIZE];
} __attribute__((packed)) locate_request;

typedef struct _locate_response {
	msg_hdr hdr;
	uint16_t port;
	char host_name[];
} __attribute__((packed)) locate_response;


// Key-value server request - GET or PUT operation

// Operation types
typedef enum {
	// Useful for testing communication between clients and servers
	// Also used as an "end of sequence" message when sending a set of keys during UPDATE-PRIMARY
	OP_NOOP,

	OP_GET,
	OP_PUT,

	OP_TYPE_MAX
} __attribute__((packed)) op_type;

__attribute__((unused))
static const char *op_type_str[OP_TYPE_MAX] = {
	"NOOP",
	"GET",
	"PUT"
};

// Possible results of an operation
typedef enum {
	SUCCESS,

	SERVER_FAILURE,
	KEY_NOT_FOUND,
	OUT_OF_SPACE,// not enough memory to store an item

	OP_STATUS_MAX
} __attribute__((packed)) op_status;

__attribute__((unused))
static const char *op_status_str[OP_STATUS_MAX] = {
	"Success",

	"Server failure",
	"Key not found",
	"Out of space"
};

typedef struct _operation_request {
	msg_hdr hdr;
	char key[KEY_SIZE];
	op_type type;
	char value[];
} __attribute__((packed)) operation_request;

typedef struct _operation_response {
	msg_hdr hdr;
	op_status status;
	char value[];
} __attribute__((packed)) operation_response;


// Control requests serviced by the metadata server

// Request types (as described in the assignment handout)
typedef enum {
	HEARTBEAT,// for failure detection

	UPDATED_PRIMARY,
	UPDATE_PRIMARY_FAILED,

	UPDATED_SECONDARY,
	UPDATE_SECONDARY_FAILED,

	MSERVER_CTRLREQ_TYPE_MAX
} __attribute__((packed)) mserver_ctrlreq_type;

__attribute__((unused))
static const char *mserver_ctrlreq_type_str[MSERVER_CTRLREQ_TYPE_MAX] = {
	"HEARTBEAT",

	"UPDATED-PRIMARY",
	"UPDATE-PRIMARY failed",

	"UPDATED-SECONDARY",
	"UPDATE-SECONDARY failed"
};

typedef struct _mserver_ctrl_request {
	msg_hdr hdr;
	mserver_ctrlreq_type type;
	uint16_t server_id;
} __attribute__((packed)) mserver_ctrl_request;


// Control requests serviced by key-value servers

// Request types (as described in the assignment handout)
typedef enum {
	SET_SECONDARY,

	UPDATE_PRIMARY,
	UPDATE_SECONDARY,

	SWITCH_PRIMARY,

	SHUTDOWN,// for gracefully terminating the servers

	SERVER_CTRLREQ_TYPE_MAX
} __attribute__((packed)) server_ctrlreq_type;

__attribute__((unused))
static const char *server_ctrlreq_type_str[SERVER_CTRLREQ_TYPE_MAX] = {
	"SET-SECONDARY",

	"UPDATE-PRIMARY",
	"UPDATE-SECONDARY",

	"SWITCH-PRIMARY",

	"SHUTDOWN"
};

// Request status
typedef enum {
	CTRLREQ_SUCCESS,
	CTRLREQ_FAILURE,

	SERVER_CTRLREQ_STATUS_MAX
} __attribute__((packed)) server_ctrlreq_status;

__attribute__((unused))
static const char *server_ctrlreq_status_str[SERVER_CTRLREQ_STATUS_MAX] = {
	"Success",
	"Failure"
};

typedef struct _server_ctrl_request {
	msg_hdr hdr;
	server_ctrlreq_type type;
	// Server location (for {SET|UPDATE}_{PRIMARY|SECONDARY} requests)
	uint16_t port;
	char host_name[];
} __attribute__((packed)) server_ctrl_request;

typedef struct _server_ctrl_response {
	msg_hdr hdr;
	server_ctrlreq_status status;
} __attribute__((packed)) server_ctrl_response;


#endif// _DEFS_H_
