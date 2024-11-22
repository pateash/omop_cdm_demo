from setuptools import setup, find_packages
setup(
    name = 'L6-2_drug_analysis_top-drugs-prescribed-in-the-past-5-years',
    version = '1.0',
    packages = find_packages(include = ('l62_drug_analysis_topdrugsprescribedinthepast5years*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.24'],
    entry_points = {
'console_scripts' : [
'main = l62_drug_analysis_topdrugsprescribedinthepast5years.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
