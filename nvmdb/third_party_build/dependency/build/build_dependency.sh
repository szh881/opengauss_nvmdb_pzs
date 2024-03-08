#!/bin/bash
# *************************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2020. All rights reserved
#
#  description: the script that make install dependency
#  date: 2020-10-21
#  version: 1.0
#  history:
#
# *************************************************************************
set -e

ARCH=$(uname -m)
ROOT_DIR="${PWD}/../.."

[ -f build_all.log ] && rm -rf build_all.log
echo ------------------------------cJSON------------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../cJSON
sh build.sh -m all >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[cJSON] is " $use_tm
echo ------------------------------jemalloc---------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../jemalloc
python build.py -m all -t "release|debug" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[jemalloc] is " $use_tm
echo ------------------------------libcgroup--------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../libcgroup
python build.py -m all -t "comm" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[libcgroup] is " $use_tm
echo ------------------------------numactl----------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../numactl
python build.py -m all -t "comm|llt" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[numactl] is " $use_tm
echo ------------------------------unixodbc--------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../unixodbc
sh build.sh -m all >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[unixodbc] is " $use_tm
echo ------------------------------fio--------------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../fio
python build.py -m all -t comm >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[fio] is " $use_tm
echo ------------------------------iperf-------------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../iperf
python build.py -m all -t comm >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[iperf] is " $use_tm
echo -------------------------------llvm------------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../llvm
sh -x build.sh -m all -c comm >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo $use_tm
echo ---------------------------------six-----------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../six
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[six] $use_tm"

#第一层依赖
echo ----------------------------------librdkafka--------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../librdkafka
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[librdkafka] $use_tm"
echo -------------------------------confluent-kafka-python------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../confluent-kafka-python
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[confluent-kafka-python] $use_tm"
#无依赖
echo ---------------------------------bcrypt--------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../bcrypt
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[bcrypt] $use_tm"
echo -------------------------------------dmlc-core----------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../dmlc-core
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[dmlc-core] $use_tm"
echo ---------------------------------libedit-------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../libedit
python build.py -m all -t "comm|llt" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[libedit] $use_tm"
echo --------------------------------kafka-python---------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../kafka-python
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[kafka-python] $use_tm"
echo ----------------------------------nng------------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../nng
python build.py -m all -t "comm" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[nanomsg] $use_tm"
echo ----------------------------------netifaces----------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../netifaces
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[netifaces] $use_tm"
echo -------------------------------------psutil----------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../psutil
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[psutil] $use_tm"
echo -------------------------------------rabit----------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../rabit
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[rabit] $use_tm"
echo --------------------------------------ptyprocess-----------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../ptyprocess
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[ptyprocess] $use_tm"
echo --------------------------------------pycryptodome-----------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../pycryptodome
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[pycryptodome] $use_tm"
echo --------------------------------------pexpect---------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../pexpect
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[pexpect] $use_tm"
echo --------------------------------------tornado---------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../tornado
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[tornado] $use_tm"
echo -----------------------------------------lz4---------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../lz4
sh build.sh -m all >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[lz4] $use_tm"
echo -----------------------------------------zlib--------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../zlib
sh build.sh -m all >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[zlib] $use_tm"
echo -----------------------------------------boost-------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../boost
sh build.sh -m all >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[boost] $use_tm"
echo -----------------------------------------zstd--------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../zstd
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[zstd] $use_tm"
echo --------------------------------------kerberos-------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../kerberos
python build.py -m all -t "comm|llt" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[kerberos] $use_tm"
echo ---------------------------------------libcurl-------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../libcurl
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[libcurl] $use_tm"
echo --------------------------------------libiconv-------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../libiconv
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[libiconv] $use_tm"
echo ---------------------------------------libxml2------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../libxml2
sh build.sh -m all >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[libxml2] $use_tm"
echo ---------------------------------------nghttp2------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../nghttp2
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[nghttp2] $use_tm"
echo ----------------------------------------pcre---------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../pcre
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[pcre] $use_tm"
echo ---------------------------------------esdk_obs_api--------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../esdk_obs_api
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[esdk_obs_api] $use_tm"

echo ---------------------------------------etcd--------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../etcd
sh build.sh build "$ROOT_DIR/output/" >>../build/build_result.log
sh build.sh client "$ROOT_DIR/output/" >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[etcd] $use_tm"
echo ---------------------------------------kmc---------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../../platform/kmc
sh build.sh >>../../dependency/build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[kmc] $use_tm"
echo ---------------------------------------sqlparse-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../sqlparse
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[sqlparse] $use_tm"
echo -------------------------------xgboost----------------------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../xgboost
sh build.sh >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[libxgboost] $use_tm"
echo ---------------------------------------flit_core-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../flit_core
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[flit_core] $use_tm"
echo ---------------------------------------typing_extensions-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../typing_extensions
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[typing_extensions] $use_tm"
echo ---------------------------------------sniffio-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../sniffio
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[sniffio] $use_tm"
echo ---------------------------------------idna-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../idna
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[idna] $use_tm"
echo ---------------------------------------anyio-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../anyio
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[anyio] $use_tm"
echo ---------------------------------------h11-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../h11
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[h11] $use_tm"
echo ---------------------------------------starlette-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../starlette
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[starlette] $use_tm"
echo ---------------------------------------pydantic-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../pydantic
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[pydantic] $use_tm"
echo ---------------------------------------fastapi-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../fastapi
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[fastapi] $use_tm"
echo ---------------------------------------zipp-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../zipp
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[zipp] $use_tm"
echo ---------------------------------------importlib_metadata-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../importlib_metadata
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[importlib_metadata] $use_tm"
echo ---------------------------------------click-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../click
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[click] $use_tm"
echo ---------------------------------------uvicorn-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../uvicorn
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[uvicorn] $use_tm"
echo ---------------------------------------certifi-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../certifi
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[certifi] $use_tm"
echo ---------------------------------------charset_normalizer-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../charset_normalizer
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[charset_normalizer] $use_tm"
echo ---------------------------------------urllib3-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../urllib3
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[urllib3] $use_tm"
echo ---------------------------------------requests-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../requests
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[requests] $use_tm"
echo ---------------------------------------pyyaml-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../pyyaml
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[pyyaml] $use_tm"
echo ---------------------------------------greenlet-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../greenlet
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[greenlet] $use_tm"
echo ---------------------------------------sqlalchemy-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../sqlalchemy
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[sqlalchemy] $use_tm"
echo ---------------------------------------wcwidth-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../wcwidth
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[wcwidth] $use_tm"
echo ---------------------------------------prettytable-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../prettytable
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[prettytable] $use_tm"
echo ---------------------------------------numpy-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../numpy
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[numpy] $use_tm"
echo ---------------------------------------scipy-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../scipy
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[scipy] $use_tm"
echo ---------------------------------------prometheus-client-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../prometheus-client
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[prometheus-client] $use_tm"
echo ---------------------------------------openGauss-DBMind-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../openGauss-DBMind
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[openGauss-DBMind] $use_tm"
echo ---------------------------------------tbb-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../tbb
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[tbb] $use_tm"
echo ---------------------------------------pmdk-----------------------------------
start_tm=$(date +%s%N)
cd $(pwd)/../pmdk
sh build.sh -m build >>../build/build_result.log
end_tm=$(date +%s%N)
use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
echo "[pmdk] $use_tm"
