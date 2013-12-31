#!/usr/bin/env python

import getopt
import sys
import os
import tempfile

MERGE_TEMP_DATABASE="temp_merge_db"
MERGE_TEMP_TABLE_PREFIX="temp_"

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


def hiveOptions(config):
    """ Default options """
    ret = ''
    ret += """
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.created.files=1000000;

"""
    if config.has_key("merge_size") and config["merge_size"].isdigit() and int(config["merge_size"]) > 16*1024*1024:
        ret += "SET hive.merge.size.per.task="+config["merge_size"]+";"
    else:
        ret += "SET hive.merge.size.per.task=256000000;"

    ret += """
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=16000000;

"""
    return ret

def hiveCompressCodec(codec):
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

def hiveEnableCompress(codec):
    """ Hive enable compression """
    codec = hiveCompressCodec(codec)
    ret = ""
    ret += "SET mapred.output.compress=true;\n"
    ret += "SET mapred.output.compression.type=BLOCK;\n"
    ret += "SET mapred.output.compression.codec=" + codec + ";\n"
    ret += "SET mapred.map.output.compression.codec=" + codec + ";\n"

    return ret

def hiveGetTempDB():
    """ Return temporary database name """
    return MERGE_TEMP_DATABASE

def hiveGetTempTable(database, table):
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

def mktemp(hive):
    """ Create temporary file """
    fd, temp_path = tempfile.mkstemp(".hive", "hivemerge_", "/tmp")
    file = open(temp_path, 'w')
    file.write(hive)
    file.close()
    os.close(fd)
    return temp_path

def Debug(hive):
    """ Debug """
    print "====================== DEBUG: HiveQL ======================"
    print hive
    print "====================== DEBUG: HiveQL ======================\n"
    sys.exit(0)


def main():
    sskaje();
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

    debug = False

    compress = False
    compressCodec = ""

    database = ""
    table = ""

    partition_keys = []
    partition_values = []
    config = {}

    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-D", "--debug"):
            debug = True
        elif o in ("-c", "--compress"):
            compress = True
        elif o in ("-C", "--compress-codec"):
            compressCodec = a
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

    hive = ""
    hive += hiveOptions(config)
    if compress:
        hive += hiveEnableCompress(compressCodec) + "\n"


    # get table fields
    has_partition = False
    if (partition_keys != []):
        has_partition = True
        partitionKeyList = ', '.join(partition_keys)
        partitionPairs = []
        for i in range(len(partition_keys)):
            try:
                partitionPairs.append(partition_keys[i] + "='" + partition_values[i] + "'")
            except IndexError:
                usage("Missing value for partition key '" + partition_keys[i] + "'")

        partitionInWhere = ' AND '.join(partitionPairs)
        partitionKeyString = ', '.join(partitionPairs)


    # Create tmp database & table
    temp_table = hiveGetTempTable(database, table)
    temp_db = hiveGetTempDB()
    temp_table_full = temp_db + "." + temp_table
    original_table_full = database + "." + table
    hive += "CREATE DATABASE IF NOT EXISTS " + temp_db + ";\n"
    hive += "CREATE TABLE IF NOT EXISTS " + temp_table_full + " LIKE " + original_table_full + ";\n\n"

    # insert
    hive += "SET hive.exec.compress.output=false;\n"
    hive += "INSERT OVERWRITE TABLE " + temp_table_full
    if has_partition:
        hive += " PARTITION("+partitionKeyList+")"
    hive += " SELECT * FROM " + original_table_full
    if has_partition:
        hive += " WHERE " + partitionInWhere + ";\n\n"
    else:
        hive += ";\n\n"

    # insert back
    hive += "SET hive.exec.compress.output=true;\n"
    hive += "INSERT OVERWRITE TABLE " + original_table_full
    if has_partition:
        hive += " PARTITION("+partitionKeyList+")"
    hive += " SELECT * FROM " + temp_table_full
    if has_partition:
        hive += " WHERE " + partitionInWhere + ";\n\n"
    else:
        hive += ";\n\n"

    # clean table/partition
    hive += "USE " + temp_db + ";\n"
    if has_partition:
        hive += "ALTER TABLE " + temp_table + " DROP PARTITION(" + partitionKeyString + ");\n"
    else:
        hive += "DROP TABLE " + temp_table + ";\n"

    # Debug only
    if debug:
        Debug(hive)


    # Create temporary file
    temp_path = mktemp(hive)
    print "File written: " + temp_path + "\n"

    # execute
    command = "hive -f " + temp_path
    print "Executing command: " + command
    run_command(command)

    # clean
    os.remove(temp_path)
    print "File removed: " + temp_path + "\n"



if __name__ == "__main__":
    main()

# EOF
