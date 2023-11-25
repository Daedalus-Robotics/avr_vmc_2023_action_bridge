from setuptools import find_packages, setup

package_name = 'avr_vmc_2023_action_bridge'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Hunter Baker',
    maintainer_email='hunterbaker@me.com',
    description='',
    license='LGPL-3.0-only',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'action_bridge_node = avr_vmc_2023_action_bridge.action_bridge:main'
        ],
    },
)
