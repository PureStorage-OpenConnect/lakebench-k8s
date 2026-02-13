# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for building the lakebench single-file binary.

Build:
    pip install pyinstaller
    pyinstaller lakebench.spec

Output:
    dist/lakebench (single executable)
"""

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Collect pydantic and pydantic-settings data files (validators, schemas)
pydantic_datas = collect_data_files("pydantic")
pydantic_settings_datas = collect_data_files("pydantic_settings")

a = Analysis(
    ["src/lakebench/__main__.py"],
    pathex=["src"],
    binaries=[],
    datas=[
        # Jinja2 templates for K8s manifests
        ("src/lakebench/templates", "lakebench/templates"),
        # Spark pipeline scripts submitted to driver pods
        ("src/lakebench/spark/scripts", "lakebench/spark/scripts"),
    ]
    + pydantic_datas
    + pydantic_settings_datas,
    hiddenimports=[
        # Core dependencies
        "kubernetes",
        "kubernetes.client",
        "kubernetes.config",
        "boto3",
        "botocore",
        "pydantic",
        "pydantic._internal",
        "pydantic_settings",
        "typer",
        "rich",
        "jinja2",
        "yaml",
        "httpx",
        # Submodules that dynamic imports may miss
        *collect_submodules("pydantic"),
        *collect_submodules("pydantic_settings"),
        *collect_submodules("kubernetes.client"),
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Test / dev-only packages
        "pytest",
        "mypy",
        "ruff",
        "moto",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="lakebench",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
