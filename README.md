[![Build Status](https://travis-ci.org/do-aki/SwitchMaster.png?branch=modulize)](https://travis-ci.org/do-aki/SwitchMaster)

# NAME

SwitchMaster - master n : slave 1 replication for mysql

# SYNOPSIS

   % carton install
   % carton exec perl switch_master.pl

# HOW TO USE

1. あらかじめ、複数のマスタからデータをバックアップし、MASTER_LOG_FILE / MASTER_LOG_POS を記録しておく。
2. スレーブサーバに、全データを投入。どれか一つのマスタに対して、レプリケーションを張っておく
3. data/settings.yaml.dist を data/settings.yaml に書き換えて、スレーブサーバへの接続設定をする。
4. data/masters.yaml.dist を data/masters.yaml に書き換えて、レプリケーション設定 (CHANGE MASTER TO の内容)をする。
   このとき、既に動いているレプリケーションの MASTER_LOG_FILE / MASTER_LOG_POS は設定する必要がない
5. switch_master.pl を実行する

# REFERENCE
* http://www.slideshare.net/do_aki/20110809-my-sql-casual-talks-vol2
* http://www.slideshare.net/do_aki/n1-12603071
* http://www.slideshare.net/do_aki/n1-19006920

# LICENSE

Copyright (C) do-aki.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

do_aki <do.hiroaki at gmail.com>

