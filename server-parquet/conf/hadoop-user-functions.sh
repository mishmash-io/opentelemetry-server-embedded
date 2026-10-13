#!/usr/bin/env bash
#
#    Copyright 2026 Mishmash IO UK Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# original function adds a number of 'add-opens' params, drop them
function hadoop_finalize_jpms_opts
{
    hadoop_debug "Not setting add-opens jvm args"
}

function hadoop_finalize_hadoop_opts
{
    hadoop_debug "Not setting additional hadoop.* opts"
}

function hadoop_add_common_to_classpath
{
    hadoop_debug "Not adding common Hadoop libraries to classpath"
}

# Don't add the HADOOP_HOME/conf dir to the classpath
function hadoop_finalize_classpath
{
    hadoop_debug "Not adding hadoop common libraries to classpath"
    
    # copied from hadoop-functions.sh without the HADOOP_HOME/conf part
    hadoop_add_to_classpath_userpath
    hadoop_translate_cygwin_path CLASSPATH true
}

function hadoop_verify_confdir
{
    hadoop_debug "Not verifying Hadoop configuration directory"
}
