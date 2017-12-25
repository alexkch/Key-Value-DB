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


#ifndef _HASH_H_
#define _HASH_H_

#include <stdbool.h>
#include <pthread.h>

#include "defs.h"
#include "dlist.h"


typedef struct _hash_entry {
	dlist_entry list_entry;
	char key[KEY_SIZE];
	void *value;// must be != NULL
	size_t value_sz;
} hash_entry;

typedef struct _hash_bucket {
	dlist entries;
	pthread_mutex_t lock;
} hash_bucket;

typedef struct _hash_table {
	size_t size;
	hash_bucket *buckets;
} hash_table;


// Initialize a hash table with given size; returns true on success
bool hash_init(hash_table *table, size_t size);

// Free resources used by a hash table
void hash_cleanup(hash_table *table);


// Lock a particular key (lock corresponding hash bucket)
void hash_lock(hash_table *table, const char key[KEY_SIZE]);

// Unlock a particular key (unlock corresponding hash bucket)
void hash_unlock(hash_table *table, const char key[KEY_SIZE]);


// Get value for a key; returns true on success; not synchronized
bool hash_get(hash_table *table, const char key[KEY_SIZE], void **value, size_t *value_sz);

// Put a new value for a key and obtain the old value (if any); returns true on success; not synchronized
bool hash_put(hash_table *table, const char key[KEY_SIZE], void *value, size_t value_sz,
              void **old_value, size_t *old_value_sz);

// Remove a key and obtain the old value (if any); returns true on success; not synchronized
bool hash_remove(hash_table *table, const char key[KEY_SIZE], void **old_value, size_t *old_value_sz);


typedef void hash_iterator(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg);

// Iterate through all keys, calling iterator(key, value, value_sz, arg) for each key; synchronized
void hash_iterate(hash_table *table, hash_iterator *iterator, void *arg);


#endif// _HASH_H_
