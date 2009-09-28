from setuptools import setup, find_packages
setup(name="mcp", version="5.2",
        packages=find_packages(exclude=('mcp_test', 'mcp_test.*')))
