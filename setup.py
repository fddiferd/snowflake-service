from setuptools import setup, find_packages

setup(
    name='your_package_name',  # Replace with your actual package name
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'snowflake-connector-python>=2.4.0',
        'pandas>=1.1.0',
        'cryptography>=3.2',
        'python-dotenv>=0.15.0',
    ],
    author='Your Name',
    author_email='your.email@example.com',
    description='A Python package for Snowflake services',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/your_package',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
