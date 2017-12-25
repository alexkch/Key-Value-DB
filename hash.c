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


#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"


// Initialize a hash table with given size; returns true on success
bool hash_init(hash_table *table, size_t size)
{
	assert(table != NULL);
	assert(size != 0);

	if ((table->buckets = malloc(size * sizeof(hash_bucket))) == NULL) {
		perror("malloc");
		return false;
	}

	table->size = size;
	for (size_t i = 0; i < size; i++) {
		dlist_init(&(table->buckets[i].entries));
		pthread_mutex_init(&(table->buckets[i].lock), NULL);
	}
	return true;
}

// Free resources used by a hash table
void hash_cleanup(hash_table *table)
{
	assert(table != NULL);

	for (size_t i = 0; i < table->size; i++) {
		hash_bucket *bucket = &(table->buckets[i]);
		while (!dlist_is_empty(&(bucket->entries))) {
			dlist_entry *l_entry = dlist_remove_head(&(bucket->entries));
			hash_entry *h_entry = container_of(l_entry, hash_entry, list_entry);
			free(h_entry);
		}
		pthread_mutex_destroy(&(bucket->lock));
	}

	free(table->buckets);
	table->buckets = NULL;
	table->size = 0;
}


// Hash function
static size_t hash_f(const char key[KEY_SIZE], size_t size)
{
	assert(key != NULL);
	assert(size > 0);

	size_t h = 0;
	for (int i = 0; i < KEY_SIZE; i++) {
		h += key[i];
	}
	return h % size;
}

// Key comparison function
static bool equals_f(const char key1[KEY_SIZE], const char key2[KEY_SIZE])
{
	assert(key1 != NULL);
	assert(key2 != NULL);

	return (key1 == key2) || (memcmp(key1, key2, KEY_SIZE) == 0);
}


// Get bucket index for a key
static size_t get_index(const hash_table *table, const char key[KEY_SIZE])
{
	assert(table != NULL);

	size_t result = hash_f(key, table->size);
	assert(result < table->size);
	return result;
}

// Find a hash entry with given key in a bucket
static hash_entry *get_entry(const hash_bucket *bucket, const char key[KEY_SIZE])
{
	assert(bucket != NULL);

	for (dlist_entry *e = bucket->entries.head; e != &(bucket->entries); e = e->next) {
		hash_entry *entry = container_of(e, hash_entry, list_entry);
		if (equals_f(entry->key, key)) {
			return entry;
		}
	}
	return NULL;
}

// Create a new hash entry and put it into the bucket
static hash_entry *put_new_entry(hash_bucket *bucket, const char key[KEY_SIZE], void *value, size_t value_sz)
{
	assert(key != NULL);
	assert(value != NULL);

	hash_entry *entry = malloc(sizeof(hash_entry));
	if (entry == NULL) {
		perror("malloc");
		return NULL;
	}

	memcpy(entry->key, key, KEY_SIZE);
	entry->value = value;
	entry->value_sz = value_sz;

	//dlist_insert_tail(&(bucket->entries), &(entry->list_entry));
	dlist_insert_head(&(bucket->entries), &(entry->list_entry));
	return entry;
}

// Update an exising hash entry with a new value, and obtain the old value
static void update_entry(hash_entry *entry, void *value, size_t value_sz, void **old_value, size_t *old_value_sz)
{
	assert(entry != NULL);
	assert(value != NULL);

	if (old_value != NULL) {
		assert(old_value_sz != NULL);
		*old_value = entry->value;
		*old_value_sz = entry->value_sz;
	}
	entry->value = value;
	entry->value_sz = value_sz;
}

// Remove a hash entry and obtain its old value
static void remove_entry(hash_entry *entry, void **old_value, size_t *old_value_sz)
{
	assert(entry != NULL);

	dlist_remove_entry(&(entry->list_entry));
	if (old_value != NULL) {
		assert(old_value_sz != NULL);
		*old_value = entry->value;
		*old_value_sz = entry->value_sz;
	}
	free(entry);
}


// Lock a particular key (lock corresponding hash bucket)
void hash_lock(hash_table *table, const char key[KEY_SIZE])
{
	size_t index = get_index(table, key);
	hash_bucket *bucket = &(table->buckets[index]);
	pthread_mutex_lock(&(bucket->lock));
}

// Unlock a particular key (unlock corresponding hash bucket)
void hash_unlock(hash_table *table, const char key[KEY_SIZE])
{
	size_t index = get_index(table, key);
	hash_bucket *bucket = &(table->buckets[index]);
	pthread_mutex_unlock(&(bucket->lock));
}


// Get value for a key; returns true on success; not synchronized
bool hash_get(hash_table *table, const char key[KEY_SIZE], void **value, size_t *value_sz)
{
	assert(value != NULL);
	assert(value_sz != NULL);

	size_t index = get_index(table, key);
	hash_bucket *bucket = &(table->buckets[index]);
	hash_entry *entry = get_entry(bucket, key);

	if (entry == NULL) {
		return false;
	}
	*value = entry->value;
	*value_sz = entry->value_sz;
	return true;
}

// Put a new value for a key and obtain the old value (if any); returns true on success; not synchronized
bool hash_put(hash_table *table, const char key[KEY_SIZE], void *value, size_t value_sz,
              void **old_value, size_t *old_value_sz)
{
	size_t index = get_index(table, key);
	hash_bucket *bucket = &(table->buckets[index]);

	hash_entry *entry = get_entry(bucket, key);
	if (entry != NULL) {
		update_entry(entry, value, value_sz, old_value, old_value_sz);
		return true;
	}

	if (!put_new_entry(bucket, key, value, value_sz)) {
		return false;
	}
	if (old_value != NULL) {
		assert(old_value_sz != NULL);
		*old_value = NULL;
		*old_value_sz = 0;
	}
	return true;
}

// Remove a key and obtain the old value (if any); returns true on success; not synchronized
bool hash_remove(hash_table *table, const char key[KEY_SIZE], void **old_value, size_t *old_value_sz)
{
	size_t index = get_index(table, key);
	hash_bucket *bucket = &(table->buckets[index]);

	hash_entry *entry = get_entry(bucket, key);
	if (entry == NULL) {
		return false;
	}

	remove_entry(entry, old_value, old_value_sz);
	return true;
}


// Iterate through all keys, calling iterator(key, value, value_sz, arg) for each key; synchronized
void hash_iterate(hash_table *table, hash_iterator *iterator, void *arg)
{
	assert(table != NULL);
	assert(iterator != NULL);

	for (size_t i = 0; i < table->size; i++) {
		hash_bucket *bucket = &(table->buckets[i]);
		pthread_mutex_lock(&(bucket->lock));

		for (dlist_entry *e = bucket->entries.head; e != &(bucket->entries); e = e->next) {
			hash_entry *entry = container_of(e, hash_entry, list_entry);
			iterator(entry->key, entry->value, entry->value_sz, arg);
		}

		pthread_mutex_unlock(&(bucket->lock));
	}
}
