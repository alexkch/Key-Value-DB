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


#ifndef _DLIST_H_
#define _DLIST_H_

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>


// Doubly-linked list structures and helper functions

typedef struct _dlist_entry {
	union {
		struct _dlist_entry *next;
		struct _dlist_entry *head;
	};
	union {
		struct _dlist_entry *prev;
		struct _dlist_entry *tail;
	};
} dlist_entry;

typedef dlist_entry dlist;


// Get address of a structure by address of its field; useful for embedding list entries in structures
#define container_of(ptr, type, member)              \
({                                                   \
	const typeof(((type*)0)->member) *_mptr = (ptr); \
	(type*)((char*)_mptr - offsetof(type, member));  \
})                                                   \


static inline void dlist_init(dlist *list)
{
	assert(list != NULL);
	list->head = list->tail = list;
}

static inline bool dlist_is_empty(const dlist *list)
{
	assert(list != NULL);
	if (list->head == list) {
		assert(list->tail == list->head);
		return true;
	}
	assert(list->tail != list);
	return false;
}

static inline void dlist_insert_after(dlist_entry *place, dlist_entry *entry)
{
	assert(place != NULL);
	assert(place->next != NULL);
	assert(entry != NULL);
	entry->next = place->next;
	entry->prev = place;
	place->next = entry;
	entry->next->prev = entry;
}

static inline void dlist_insert_before(dlist_entry *place, dlist_entry *entry)
{
	dlist_insert_after(place->prev, entry);
}

static inline void dlist_insert_head(dlist *list, dlist_entry *entry)
{
	dlist_insert_after(list, entry);
}

static inline void dlist_insert_tail(dlist *list, dlist_entry *entry)
{
	dlist_insert_before(list, entry);
}

static inline dlist_entry *dlist_remove_entry(dlist_entry *entry)
{
	assert(entry != NULL);
	assert(entry->next != NULL);
	assert(entry->prev != NULL);
	if (entry->next == entry) {
		assert(entry->prev == entry);
		return NULL;
	}
	entry->next->prev = entry->prev;
	entry->prev->next = entry->next;
	return entry;
}

static inline dlist_entry *dlist_remove_head(dlist *list)
{
	return dlist_remove_entry(list->head);
}

static inline dlist_entry *dlist_remove_tail(dlist *list)
{
	return dlist_remove_entry(list->tail);
}


#endif// _DLIST_H_
