#!/usr/bin/env python3

import argparse
import functools
import logging
import os
import subprocess

logging.basicConfig(level=logging.INFO)


@functools.lru_cache(1024)
def get_package_info(package):
    logging.info('Run `pip show %s`', package)
    package_info_lines = subprocess.check_output(['pip', 'show', package]).decode('utf-8').split('\n')
    package_info = {}

    for line in package_info_lines:
        key, sep, value = line.partition(':')
        if not sep:
            continue
        key = key.strip().lower()
        value = value.strip()

        if key in ('requires', 'required-by'):
            value = [
                val.strip()
                for val in value.split(',')
                if val.strip()
            ]

        package_info[key] = value

    return package_info


def get_pip_freeze():
    for line in subprocess.check_output(['pip', 'freeze']).decode('utf-8').split('\n'):
        package, sep, version = line.strip().partition('==')
        if not sep:
            continue
        yield (package, version)


def parse_requirements(requirements_fp):
    main_packages = []
    freezed_packages = []
    custom_packages = []
    current_packages = main_packages

    for line in requirements_fp:
        if line.startswith('# -- pip freezed'):
            current_packages = freezed_packages
        elif line.startswith('#'):
            continue
        else:
            package, sep, version = line.partition('==')
            if sep:
                current_packages.append(package)
            else:
                custom_packages.append(package)

    return main_packages, custom_packages, freezed_packages


def match_custom_package(package, custom_packages):
    for custom_package in custom_packages:
        if package in custom_package:
            return True
    return False


def get_all_dependencies(package):
    deps = set()
    package_info = get_package_info(package)
    package_requires = package_info.get('requires', [])
    deps.update(package_requires)
    for dep_package in package_requires:
        deps.update(get_all_dependencies(dep_package))
    return deps


def freeze_requirements(main_packages, custom_packages):
    main_set = set(main_packages)
    used_main_set = set()

    main_requirements = []
    freezed_requirements = []
    required_packages = set()
    freezed_canditates = []
    for package, version in get_pip_freeze():
        if package in main_set:
            main_requirements.append(f'{package}=={version}')
            required_packages.update(get_all_dependencies(package))
            used_main_set.add(package)
        elif match_custom_package(package, custom_packages):
            required_packages.update(get_all_dependencies(package))
        else:
            freezed_canditates.append((package, version))

    for package, version in freezed_canditates:
        if package in required_packages:
            freezed_requirements.append(f'{package}=={version}')

    main_requirements.sort(key=str.lower)
    custom_packages.sort(key=str.lower)
    freezed_requirements.sort(key=str.lower)

    if len(main_set) > len(used_main_set):
        raise RuntimeError(f'Main dependencies not installed: {sorted(main_set - used_main_set)}')

    return main_requirements, custom_packages, freezed_requirements


def save_freezed_requirements(requirements_fp, main_requirements, custom_packages, freezed_requirements):
    for requirement in main_requirements:
        requirements_fp.write(requirement)
        requirements_fp.write('\n')

    if custom_packages:
        requirements_fp.write('# -- custom\n')

        for requirement in custom_packages:
            requirements_fp.write(requirement)
            requirements_fp.write('\n')

    if freezed_requirements:
        requirements_fp.write('# -- pip freezed\n')

        for requirement in freezed_requirements:
            requirements_fp.write(requirement)
            requirements_fp.write('\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('requirements_path', nargs='?', default='requirements.txt')
    parser.add_argument('--keep-orig', action='store_true', default=False)

    args = parser.parse_args()

    requirements_path = os.path.abspath(args.requirements_path)
    if not os.path.exists(requirements_path):
        raise RuntimeError(f'Not existent requirements path: {requirements_path}')

    with open(requirements_path) as fp:
        main_packages, custom_packages, freezed_packages = parse_requirements(fp)

    main_requirements, custom_packages, freezed_requirements = freeze_requirements(main_packages, custom_packages)

    os.rename(requirements_path, requirements_path + '.orig')
    with open(requirements_path, 'w') as fp:
        save_freezed_requirements(fp, main_requirements, custom_packages, freezed_requirements)

    if not args.keep_orig:
        os.remove(requirements_path + '.orig')


if __name__ == '__main__':
    main()
