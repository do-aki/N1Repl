[![Build Status](https://travis-ci.org/do-aki/N1Repl.svg?branch=master)](https://travis-ci.org/do-aki/N1Repl)
[![Coverage Status](https://coveralls.io/repos/do-aki/N1Repl/badge.png?branch=master)](https://coveralls.io/r/do-aki/N1Repl?branch=master)

# NAME

SwitchMaster - master n : slave 1 replication for mysql

# SYNOPSIS

   % carton install

   % carton exec perl n1repl_manager.pl --conf=config.yaml

# INSTALLATION 

Carton をインストールして
```
$ curl -L http://cpanmin.us | perl - --sudo App::cpanminus
$ sudo cpanm install Carton
```

git clone & carton install するだけ
```
$ git clone https://github.com/do-aki/N1Repl.git n1repl
$ cd n1repl
$ carton install
```

テスト
```
$ carton exec prove -lv # test
```

n1repl 実行
```
$ perl n1repl_manager.pl --conf=data/config.yaml
```


### とりあえず動かしたい人向け
テストいらないなら DBD::mysql と YAML::Tiny だけあれば良いので、以下でもOK

####Red Hat 系 (CentOS, Fedora 等)
```
$ sudo yum install git perl-DBD-MySQL perl-YAML-Tiny
$ git clone https://github.com/do-aki/N1Repl.git n1repl
$ cd n1repl
$ perl n1repl_manager.pl --conf=data/config.yaml
```

####Debian 系 (Ubuntu, Raspbian 等)
```
$ sudo apt-get install git libdbd-mysql-perl libyaml-tiny-perl
$ git clone https://github.com/do-aki/N1Repl.git n1repl
$ cd n1repl
$ perl n1repl_manager.pl --conf=data/config.yaml
```

# HOW TO USE

1. あらかじめ、複数のマスタからデータをバックアップし、その時点の MASTER_LOG_FILE / MASTER_LOG_POS を記録しておく。
2. スレーブサーバに、全データを投入。どれか一つのマスタに対して、レプリケーションを張っておく
3. data/config.yaml を修正し、スレーブサーバへの接続設定をする
4. data/masters.yaml.dist を data/masters.yaml に書き換えて、レプリケーション設定 (CHANGE MASTER TO の内容)をする。
   このとき、既に動いているレプリケーションの MASTER_LOG_FILE / MASTER_LOG_POS は適当な値を設定しておいてください
5. `n1repl_manager.pl` を `--conf` オプションを付けて実行する

実行例
```
$ perl n1repl_manager.pl --conf=data/config.yaml > n1repl.log &
$ disown
```

## n1repl_command.pl

n1repl_manager 実行中に、その挙動を制御できます

```
perl n1repl_manager.pl command [options]
```
## コマンド一覧
### stop
n1repl によるマスタ切り替えを一時的に停止します

### start
n1repl によるマスタ切り替えを再開します

### switch
切り替え対象のマスタの一台を、別のマスタに入れ替えます (試験的機能)

* --orig_master_host=現在のマスタhost
* --orig_master_port=現在のマスタport
* --orig_master_log_file=現在のマスタ ログファイル
* --orig_master_log_pos=現在のマスタ ログポジション
* --new_master_host=入れ替え先のマスタhost
* --new_master_port=入れ替え先のマスタport
* --new_master_log_file=入れ替え先のマスタ ログファイル
* --new_master_log_pos=入れ替え先のマスタ ログポジション


# CONFIGURATION

## data/config.yaml
```
data_file: マスタ情報が記録されるyamlを指定 (data/masters.yaml のままでよい)
command_file: n1repl_command のコマンド発行用のディレクトリ (data/command のままでよい)
mysql:
  MYSQL_HOST: (複数のマスタからデータを受け取る)スレーブのhostを指定
  MYSQL_PORT: (複数のマスタからデータを受け取る)スレーブのportを指定
  MYSQL_USER: n1repl を実行する MySQL の ユーザ (下記参照)
  MYSQL_PASSWORD: pass
```

n1repl を実行する MySQL の ユーザ は、以下の SQL を実行する権限を持っている必要がある

* SHOW SLAVE STATUS 
* MASTER_POS_WAIT
* START SLAVE
* STOP SLAVE
* CHANGE MASTER TO


# REFERENCE
* http://www.slideshare.net/do_aki/20110809-my-sql-casual-talks-vol2
* http://www.slideshare.net/do_aki/n1-replication-meets-mha

# LICENSE

Copyright (C) do-aki.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

do_aki <do.hiroaki at gmail.com>

