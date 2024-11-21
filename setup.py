from setuptools import setup, find_packages

def load_requirements():
    with open("requirements.txt") as f:
        return f.read().splitlines()

setup(
    name='rps',
    version='0.1',
    packages=find_packages(where="src"),  # Specify src as the root for packages
    package_dir={"": "src"},  # Map package root to src directory
    include_package_data=True,
    install_requires=load_requirements(),
    entry_points={
        'console_scripts': [
            'collect=data_collection.collect_data:main',  # Update to match the package path
        ],
    },
)
