from setuptools import setup, find_packages
setup(
    name = 'L3-2_omop-vocab-setup_create_vocab_map_tables',
    version = '1.0',
    packages = find_packages(include = ('l32_omopvocabsetup_create_vocab_map_tables*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.24'],
    entry_points = {
'console_scripts' : [
'main = l32_omopvocabsetup_create_vocab_map_tables.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
