from setuptools import setup


setup(
    name='cldfbench_amsd',
    py_modules=['cldfbench_amsd'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'amsd=cldfbench_amsd:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
