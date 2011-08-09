MySQL Casual Talks Vol.2 で話した、Slave の、参照先 Master 切り替えツール


====================
必要なモノ
====================
perl と、perl の DBI モジュールと DBD::mysql モジュールが必要


switch_master.pl 35行目付近の

SwitchMaster::DBI を SwitchMaster::Command

にすると、 mysql クライアントを利用して実行する。
(その場合は DBI/DBD::mysql 必要ない)


====================
どうやって使う？
====================

1.
	あらかじめ、複数のマスタからデータをバックアップし、
	MASTER_LOG_FILE / MASTER_LOG_POS を記録しておく。

2.
	スレーブサーバに、全データを投入。
	どれか一つのマスタに対して、レプリケーションを張っておく

3.
	data/settings.yaml.dist を data/settings.yaml に書き換えて、スレーブサーバへの接続設定をする。

	data/masters.yaml.dist を data/masters.yaml に書き換えて、レプリケーション設定 (CHANGE MASTER TO の内容)をする。
	このとき、既に動いているレプリケーションの MASTER_LOG_FILE / MASTER_LOG_POS は設定する必要がない

4.
	perl switch_master.pl

デーモン化とかしてないので、nohup するなり disown するなりしてください。

