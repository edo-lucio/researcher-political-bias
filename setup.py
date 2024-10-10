from setuptools import setup, find_packages

def load_requirements():
    with open("requirements.txt") as f:
        return f.read().splitlines()

setup(
    name='political_bias_prediction',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=load_requirements(),
    entry_points={
        'console_scripts': [
        ],
    },
)
