#!/usr/bin/env python

import getopt
import sys
import os
import tempfile

MERGE_TEMP_DATABASE = "temp_merge_db"
MERGE_TEMP_TABLE_PREFIX = "temp_"


def sskaje():
    print "\nHive Merge v0.1 \n  Author: sskaje (https://sskaje.me/)\n"


def usage(error=""):
    """ Usage """

    if error != "":
        print "Error: " + error + "\n"

    print "Usage: python " + sys.argv[0] + " OPTIONS"
    print '    options:'
    print '      -h, --help                      Display this menu'
    print '      -D, --debug                     Debug mode, display HiveQL only'
    print '      -d, --database=database         Database name'
    print '      -t, --table=table               Table name'
    print '      -c, --compress                  Enable compression'
    print '      -C, --compress-codec=codec      Compression codec. lz4, gzip, bzip2,lzo, snappy, deflate(default).'
    print '      -p, --pk=partition_key          Partition key'
    print '      -P, --pv=partition_value        Partition value'
    print '      -S, --merge-size=merge_size     Merge size before compression, hive.merge.size.per.task, '
    print '                                         256000000 by default'
    print ''

    sys.exit(255)


def debug(hiveql):
    """ Debug """
    print "====================== DEBUG: HiveQL ======================"
    print hiveql
    print "====================== DEBUG: HiveQL ======================\n"
    sys.exit(0)


def hive_options(config):
    """ Default options """
    ret = ''
    ret += """
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.created.files=1000000;

"""
    if "merge_size" in config and config["merge_size"].isdigit() and int(config["merge_size"]) > 16*1024*1024:
        ret += "SET hive.merge.size.per.task="+config["merge_size"]+";"
    else:
        ret += "SET hive.merge.size.per.task=256000000;"

    ret += """
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=16000000;

"""

    # Hive compression
    if "compress" in config and config["compress"]:
        print "Compression enabled"
        compress_codec = ""
        if "compress_codec" in config:
            compress_codec = config["compress_codec"]
        ret += hive_enable_compression(compress_codec) + "\n"

    return ret


def hive_compress_codec(codec):
    """ Get hive compression codec """
    if codec == "lz4":
        return "org.apache.hadoop.io.compress.Lz4Codec"
    elif codec == "gzip":
        return "org.apache.hadoop.io.compress.GzipCodec"
    elif codec == "bzip2":
        return "org.apache.hadoop.io.compress.BZip2Codec"
    elif codec == "lzo":
        return "com.hadoop.compression.lzo.LzopCodec"
    elif codec == "snappy":
        return "org.apache.hadoop.io.compress.SnappyCodec"
    elif codec == "deflate" or codec == "":
        return "org.apache.hadoop.io.compress.DefaultCodec"
    else:
        usage("Invalid compression codec")


def hive_enable_compression(codec):
    """ Hive enable compression """
    codec = hive_compress_codec(codec)
    print "Compression codec=" + codec
    ret = ""
    ret += "SET mapred.output.compress=true;\n"
    ret += "SET mapred.output.compression.type=BLOCK;\n"
    ret += "SET mapred.output.compression.codec=" + codec + ";\n"
    ret += "SET mapred.map.output.compression.codec=" + codec + ";\n"

    return ret


def hive_get_temp_db():
    """ Return temporary database name """
    return MERGE_TEMP_DATABASE


def hive_get_temp_table(database, table):
    """ Return temporary table name """
    ret = MERGE_TEMP_TABLE_PREFIX
    ret += database
    ret += "_"
    ret += table
    return ret


def run_command(command):
    """ Execute command """
    f = os.popen(command)
    for i in f.readlines():
        print i


def mktemp(hiveql):
    """ Create temporary file """
    fd, temp_path = tempfile.mkstemp(".hive", "hivemerge_", "/tmp")
    f = open(temp_path, 'w')
    f.write(hiveql)
    f.close()
    os.close(fd)
    return temp_path


def main():
    sskaje()
    try:
        short_opts = "hDcC:d:t:p:P:S:"
        long_opts = [
            "help",
            "debug",
            "compress",
            "compress-codec=",
            "database=",
            "table="
            "pk="
            "pv="
            "split-size="
        ]
        opts, args = getopt.getopt(sys.argv[1:], short_opts, long_opts)
    except getopt.GetoptError as err:
        # print help information and exit:
        usage(err.msg)
        sys.exit(2)

    debug_mode = False

    config = {}

    database = ""
    table = ""

    partition_keys = []
    partition_values = []

    # Process options
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-D", "--debug"):
            debug_mode = True
        elif o in ("-c", "--compress"):
            config["compress"] = True
        elif o in ("-C", "--compress-codec"):
            config["compress_codec"] = a
        elif o in ("-d", "--database"):
            database = a
        elif o in ("-t", "--table"):
            table = a
        elif o in ("-p", "--pk"):
            partition_keys.append(a)
        elif o in ("-P", "--pv"):
            partition_values.append(a)
        elif o in ("-S", "--merge-size"):
            config["merge_size"] = a

    if database == "" or table == "":
        usage("database and table are required.")

    hiveql = ""
    # Hive configurations
    hiveql += hive_options(config)

    # Build partition statements
    has_partition = False
    partition_in_where = ""
    partition_key_list = ""
    partition_key_string = ""

    if len(partition_keys):
        has_partition = True
        partition_key_list = ', '.join(partition_keys)
        partition_pairs = []
        for i in range(len(partition_keys)):
            try:
                partition_pairs.append(partition_keys[i] + "='" + partition_values[i] + "'")
            except IndexError:
                usage("Missing value for partition key '" + partition_keys[i] + "'")

        partition_in_where = ' AND '.join(partition_pairs)
        partition_key_string = ', '.join(partition_pairs)

    # Create tmp database & table
    temp_table = hive_get_temp_table(database, table)
    temp_db = hive_get_temp_db()
    temp_table_full = temp_db + "." + temp_table
    original_table_full = database + "." + table
    hiveql += "CREATE DATABASE IF NOT EXISTS " + temp_db + ";\n"
    hiveql += "CREATE TABLE IF NOT EXISTS " + temp_table_full + " LIKE " + original_table_full + ";\n\n"

    # Insert to temporary table
    hiveql += "SET hive.exec.compress.output=false;\n"
    hiveql += "INSERT OVERWRITE TABLE " + temp_table_full
    if has_partition:
        hiveql += " PARTITION("+partition_key_list+")"
    hiveql += " SELECT * FROM " + original_table_full
    if has_partition:
        hiveql += " WHERE " + partition_in_where + ";\n\n"
    else:
        hiveql += ";\n\n"

    # Insert back from temporary table
    hiveql += "SET hive.exec.compress.output=true;\n"
    hiveql += "INSERT OVERWRITE TABLE " + original_table_full
    if has_partition:
        hiveql += " PARTITION("+partition_key_list+")"
    hiveql += " SELECT * FROM " + temp_table_full
    if has_partition:
        hiveql += " WHERE " + partition_in_where + ";\n\n"
    else:
        hiveql += ";\n\n"

    # Clean table/partition
    hiveql += "USE " + temp_db + ";\n"
    if has_partition:
        hiveql += "ALTER TABLE " + temp_table + " DROP PARTITION(" + partition_key_string + ");\n"
    else:
        hiveql += "DROP TABLE " + temp_table + ";\n"

    # Debug only
    if debug_mode:
        debug(hiveql)

    # Create temporary file
    temp_path = mktemp(hiveql)
    print "File written: " + temp_path + "\n"

    # Execute command
    command = "hive -f " + temp_path
    print "Executing command: " + command
    run_command(command)

    # Clean temporary file
    os.remove(temp_path)
    print "File removed: " + temp_path + "\n"

if __name__ == "__main__":
    main()

# EOF
