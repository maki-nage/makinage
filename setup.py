import sys

try:
    from setuptools import setup, find_packages
    use_setuptools = True
except ImportError:
    from distutils.core import setup
    use_setuptools = False

try:
    with open('README.rst', 'rt') as readme:
        description = '\n' + readme.read()
except IOError:
    # maybe running setup.py from some other dir
    description = ''

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []

python_requires = '>=3.6'
install_requires = [
    'Rx>=3.0',
    'PyYAML>=5.3',
    'cyclotron>=1.2',
    'cyclotron-std>=1.0',
    'cyclotron-aiokafka>=0.3',
    'cyclotron-consul>=0.1',
]

setup(
    name="makinage",
    version='0.6.0',
    url='https://github.com/maki-nage/makinage.git',
    license='MIT',
    description="Reactive data science",
    long_description=description,
    author='Romain Picard',
    author_email='romain.picard@oakbits.com',
    packages=find_packages(),
    install_requires=install_requires,
    setup_requires=pytest_runner,
    tests_require=['pytest>=5.0.1'],
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
    project_urls={
        'Documentation': 'https://makinage.readthedocs.io',
    },
    entry_points={
        'console_scripts': [
            'makinage=makinage.makinage:main',
            'makinage-model-publisher=makinage.model_publisher.model_publisher:main',
        ],
    }
)
