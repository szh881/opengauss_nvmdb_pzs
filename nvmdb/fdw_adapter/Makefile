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
# Makefile
#     Makefile for the nvmdb FDW adapter
# 
# IDENTIFICATION
#        src/gausskernel/storage/nvmdb/fdw_adapter/Makefile
#
# ---------------------------------------------------------------------------------------

MODULE_big = nvmdb_fdw

EXTENSION = nvmdb_fdw

REGRESS = nvmdb_fdw

subdir=src/gausskernel/storage/nvmdb/fdw_adapter
top_builddir ?= ../../../../../

ENGINE_INC = $(top_builddir)/src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include
THIRD_PARTY_INC = $(top_builddir)/src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties


include $(top_builddir)/src/Makefile.global

OBJ_DIR = ./obj

SRCS := $(wildcard *.cpp)
OBJS := $(patsubst %.cpp, $(OBJ_DIR)/%.o, $(SRCS))
DEPS := $(patsubst %.cpp, $(OBJ_DIR)/%.d, $(SRCS))

DATA = nvm_fdw.control nvm_fdw--1.0.sql

include $(top_srcdir)/src/gausskernel/common.mk
override CXXFLAGS += -I$(TBB_INCLUDE_PATH) -I$(PMDK_INCLUDE_PATH) -I$(top_builddir)/src/gausskernel/storage/nvmdb/fdw_adapter -I$(ENGINE_INC) 
override CXXFLAGS +=  -I$(ENGINE_INC)/codec -I$(ENGINE_INC)/heap -I$(ENGINE_INC)/index -I$(ENGINE_INC)/table_space -I$(ENGINE_INC)/undo
override CXXFLAGS +=  -I$(THIRD_PARTY_INC)/pactree/include -I$(THIRD_PARTY_INC)/pactree/src -I$(THIRD_PARTY_INC)/pactree/lib/PDL-ART

$(OBJS): | buildrepo

install: install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall: uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

clean: 
	rm -rf $(OBJ_DIR)

buildrepo:
	@$(call make-repo)

$(OBJ_DIR)/%.o: %.cpp
	$(COMPILE.cpp) -std=c++14 -MMD -MP -MF"$(patsubst %.o,%.d,$@)" -MT"$@" -o $@ $<

show:
	@echo "CC_VERSION=${CC_VERSION}"
	@echo "enable_cassert=${enable_cassert}"
	@echo "CC=${CC}"
	@echo
	@echo "DEBUG=${DEBUG}"
	@echo
	@echo "BUILD=${BUILD}"
	@echo
	@echo "SRC_DIRS=${SRC_DIRS}"
	@echo
	@echo "CPPFLAGS=${CPPFLAGS}"
	@echo
	@echo "CFLAGS=${CFLAGS}"
	@echo
	@echo "LDFLAGS=${LDFLAGS}"
	@echo
	@echo "CXXFLAGS=${CXXFLAGS}"
	@echo
	@echo "CPPS=${CPPS}"
	@echo
	@echo "OBJS=${OBJS}"

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

define make-repo
   mkdir -p $(OBJ_DIR); 
endef

