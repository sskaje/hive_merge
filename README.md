Merge small files on HDFS for Hive table
==========

Solving small files problems on HDFS for Hive table.

Author: sskaje ([http://sskaje.me/](http://sskaje.me/))

More on my blog: [https://sskaje.me/2013/12/project-merging-small-files-hdfs-hive-table/](https://sskaje.me/2013/12/project-merging-small-files-hdfs-hive-table/)


## Usage
<pre>
Hive Merge v0.1 
  Author: sskaje (https://sskaje.me/)

Error: database and table are required.

Usage: python merge.py OPTIONS
    options:
      -h, --help                      Display this menu
      -D, --debug                     Debug mode, display HiveQL only
      -d, --database=database         Database name
      -t, --table=table               Table name
      -c, --compress                  Enable compression
      -C, --compress-codec=codec      Compression codec. lz4, gzip, bzip2,lzo, snappy, deflate(default).
      -p, --pk=partition_key          Partition key
      -P, --pv=partition_value        Partition value
      -S, --merge-size=merge_size     Merge size before compression, hive.merge.size.per.task, 
                                         256000000 by default
</pre>

## Examples

<pre>
# Merge files in a table
sudo -u hdfs python merge.py  -d lecai_ad -t ext_ad_show
</pre>

<pre>
# Merge files in a table, lz4 compressed
sudo -u hdfs python merge.py  -d lecai_ad -t ext_ad_show -c -C lz4
</pre>

<pre>
# Merge files in a partition(entry_date='2013-12-29'), lz4 compressed
sudo -u hdfs python merge.py  -d lecai_ad -t ext_ad_show -p entry_date -P '2013-12-29' -c -C lz4
</pre>

<pre>
# Merge files in a partition(entry_date='2013-12-29', type='1'), lz4 compressed
sudo -u hdfs python merge.py  -d lecai_ad -t ext_ad_show -p entry_date -P '2013-12-29' -p type -P 1 -c -C lz4
</pre>

\#EOF