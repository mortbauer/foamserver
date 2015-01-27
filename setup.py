from setuptools import setup
setup(
    name='foamserver',
    description='openfoam observer and eventually controller',
    license='LGPL',
    version = 0.0,
    author = 'Martin Ortbauer',
    author_email = 'mortbauer@gmail.com',
    url='http://github/mortbauer/foamserver',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: LGPL License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development',
        ],
    packages =['foamserver',
               'foamserver.server',
               'foamserver.harvester',
               'foamserver.foamparser',
               'foamserver.postpro',
               ],
    platforms='any',
    entry_points = {
        'console_scripts' :[
            'foamserver_server = foamserver.server:run',
            'foamserver_harvester = foamserver.harvester:run',
        ]},
    )

