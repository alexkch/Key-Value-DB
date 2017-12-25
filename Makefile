# Copyright (C) 2016, 2017 Alexey Khrabrov, Bogdan Simion
#
# Distributed under the terms of the GNU General Public License.
#
# This file is part of Assignment 3, CSC469, Fall 2017.
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This file is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this file.  If not, see <http://www.gnu.org/licenses/>.


CC = gcc
CFLAGS = -g -Wall -Wextra -std=gnu99
LDFLAGS = -pthread -lrt

CLIENT_EXE = client
CLIENT_SRC = client.c md5.c util.c

MSERVER_EXE = mserver
MSERVER_SRC = mserver.c util.c

SERVER_EXE = server
SERVER_SRC = server.c util.c hash.c

TARGETS = CLIENT MSERVER SERVER

CLEAN_FILES = *.log
CLEAN_DIRS = util util/collections

$(foreach t, $(TARGETS), $(eval $t_OBJ = $($t_SRC:.c=.o)))

ALL_EXE = $(foreach t, $(TARGETS), $($t_EXE))
ALL_OBJ = $(foreach t, $(TARGETS), $($t_OBJ))

.PHONY: all clean

all: $(ALL_EXE)

$(foreach t, $(TARGETS), $(eval $($t_EXE): $($t_OBJ); $(CC) $(LDFLAGS) $$^ -o $$@))

-include $(ALL_OBJ:.o=.d)

%.o: %.c
	$(CC) $(CFLAGS) -c -MMD $< -o $@

clean:
	rm -f $(ALL_EXE) *.o *.d *~ $(CLEAN_FILES) $(foreach d, $(CLEAN_DIRS), $d/*.o $d/*.d)
