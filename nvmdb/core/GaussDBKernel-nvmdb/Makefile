#
# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
#
# Makefile.local
#     Makefile for the nvmdb storage engine core components
#
# IDENTIFICATION
#     src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/Makefile
#
# ---------------------------------------------------------------------------------------

CC=g++
CFLAGS ?= 

subdir=src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb
top_builddir ?= ../../../../../../

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/gausskernel/common.mk

.SUFFIXES:
.SUFFIXES: .o .cpp .h .hpp

#Target directory definitions.Scan them recursively.
SRC_TARGET_DIRS = \
		dbcore \
		include \
		third_parties

SRC_DIRS = $(shell find $(SRC_TARGET_DIRS) -type d 2> /dev/null) # 2> /dev/null To remove error printout when calling clean
SRC_DIRS += .
INCLUDE = $(foreach dir, $(SRC_DIRS), -I$(dir))
OBJ_DIR = ./obj

# Common flags definition
CFLAGS += -std=c++14 -w -D_GLIBCXX_USE_CXX11_ABI=0 -I$(TBB_INCLUDE_PATH) -I$(PMDK_INCLUDE_PATH) 
CFLAGS += $(INCLUDE)
# turn on some warnings
CFLAGS += -Wwrite-strings -Wcast-align -Wreturn-type
CFLAGS += -Wpointer-arith -Wlogical-op -Waddress -Wsizeof-pointer-memaccess -Winit-self
CFLAGS += -DNVMDB_ADAPTER
HAVE_CLWB = $(shell cat /proc/cpuinfo | grep clwb)
ifndef HAVE_CLWB
CFLAGS += -DNO_CLWB
endif

LDFLAGS += $(CFLAGS)

CPPS := $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.cpp))
OBJS := $(CPPS)
OBJS := $(patsubst %.cpp, $(OBJ_DIR)/%.o, $(OBJS))
DEPS := $(OBJS:.o=.d)

.PHONY: FORCE all

all: objfiles

FORCE:

$(OBJS): | buildrepo

objfiles: objfiles.txt
objfiles.txt: Makefile $(OBJS)

$(OBJ_DIR)/%.o: %.cpp
	@$(CC) -c $(CFLAGS) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -MF"$(patsubst %.o,%.d,$@)" -MT"$@" -o $@ $<

.PHONY: clean

clean:
	@rm -rf $(OBJ_DIR) objfiles.txt

buildrepo:
	@$(call make-repo)

.PHONY: show
show:
	@echo "enable_cassert=${enable_cassert}"
	@echo
	@echo "CC_VERSION=${CC_VERSION}"
	@echo
	@echo "CC=${CC}"
	@echo
	@echo "SRC_DIRS=${SRC_DIRS}"
	@echo
	@echo "CPPFLAGS=${CPPFLAGS}"
	@echo
	@echo "CXXFLAGS=${CXXFLAGS}"
	@echo
	@echo "CFLAGS=${CFLAGS}"
	@echo
	@echo "LDFLAGS=${LDFLAGS}"
	@echo
	@echo "CPPS=${CPPS}"
	@echo
	@echo "OBJS=${OBJS}"

# Optionally include dependencies files
ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

define make-repo
   mkdir -p $(OBJ_DIR); \
   mkdir -p $(BIN_DIR); \
   for dir in $(SRC_DIRS); \
   do \
	mkdir -p $(OBJ_DIR)/$$dir; \
   done
endef
