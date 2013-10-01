requires 'perl', '5.008001';
requires 'YAML::Tiny', '1.48';
requires 'DBI', '1.609';
requires 'DBD::mysql', '4.013';

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::mysqld', '0.17';
    requires 'Test::SharedFork', '0.21';
    requires 'Test::TCP', '2.01';
};

