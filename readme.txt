MySQL Casual Talks Vol.2 �Řb�����ASlave �́A�Q�Ɛ� Master �؂�ւ��c�[��


====================
�K�v�ȃ��m
====================
perl �ƁAperl �� DBI ���W���[���� DBD::mysql ���W���[�����K�v


switch_master.pl 35�s�ڕt�߂�

SwitchMaster::DBI �� SwitchMaster::Command

�ɂ���ƁA mysql �N���C�A���g�𗘗p���Ď��s����B
(���̏ꍇ�� DBI/DBD::mysql �K�v�Ȃ�)


====================
�ǂ�����Ďg���H
====================

1.
	���炩���߁A�����̃}�X�^����f�[�^���o�b�N�A�b�v���A
	MASTER_LOG_FILE / MASTER_LOG_POS ���L�^���Ă����B

2.
	�X���[�u�T�[�o�ɁA�S�f�[�^�𓊓��B
	�ǂꂩ��̃}�X�^�ɑ΂��āA���v���P�[�V�����𒣂��Ă���

3.
	data/settings.yaml.dist �� data/settings.yaml �ɏ��������āA�X���[�u�T�[�o�ւ̐ڑ��ݒ������B

	data/masters.yaml.dist �� data/masters.yaml �ɏ��������āA���v���P�[�V�����ݒ� (CHANGE MASTER TO �̓��e)������B
	���̂Ƃ��A���ɓ����Ă��郌�v���P�[�V������ MASTER_LOG_FILE / MASTER_LOG_POS �͐ݒ肷��K�v���Ȃ�

4.
	perl switch_master.pl

�f�[�������Ƃ����ĂȂ��̂ŁAnohup ����Ȃ� disown ����Ȃ肵�Ă��������B

