import argparse
import getpass
import logging
import os
import urllib

import tableauserverclient as TSC
from tableaudocumentapi import Workbook


def main():

    parser = argparse.ArgumentParser(description='dump output from workbooks')
    parser.add_argument('--server', '-s', required=True, help='server address')
    parser.add_argument('--site', '-S', default='')
    parser.add_argument('--project', required=True, default=None)
    parser.add_argument('--username', '-u', help='username to sign into server')
    parser.add_argument('-p', '--password', default=None)
    parser.add_argument('--filepath', '-f', required=True, help='filepath to save the workbooks returned')

    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')

    parser.add_argument('workbook', help='one or more workbooks to process, "all" means all workbooks (within a project)', nargs='+')

    args = parser.parse_args()

    if args.password is None:
        password = getpass.getpass("Password: ")
    else:
        password = args.password

    # Set logging level based on user input, or error by default
    logging_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=logging_level)

    # Step 1: Sign in to server.
    tableau_auth = TSC.TableauAuth(args.username, password, site_id=args.site)
    server = TSC.Server(args.server)
    # The new endpoint was introduced in Version 2.5
    server.version = "2.5"

    with server.auth.sign_in(tableau_auth):
        server.use_server_version()
        calculations = dict()
        # Step 2: Query for the workbook that we want info on
        for wb in TSC.Pager(server.workbooks):
            if (args.workbook[0] == "all" or wb.name in args.workbook) and wb.project_name == args.project:
                filename = server.workbooks.download(wb.id, filepath=args.filepath, include_extract=False)
                contents = Workbook(filename)
                print("workbook [{0}]: {1}".format(wb.name, contents.filename))
                for ds in contents.datasources:
                    for name in ds.fields:
                        field = ds.fields[name]
                        if field.calculation is not None and 'false' != field.calculation:
                            calculations[field.id] = field
                for ds in contents.datasources:
                    for name in ds.fields:
                        field = ds.fields[name]
                        s = substitute_calculations(calculations, field.calculation)
                        if len(field.worksheets) > 0:
                            print('field [{1}]({0}): {3}; "{4}":"{5}"'.format(field.datatype, field.name, field.caption, ", ".join(field.worksheets), field.calculation, s))


def substitute_calculations(calculations, calculation):
    # brute force: just try to replace all calculations
    if calculation is not None and 'false' != calculation:
        substitution = True
        while substitution:
            substitution = False
            for c, value in calculations.items():
                calc_new = calculation.replace(c, "(" + value.calculation + ")")
                if calc_new != calculation:
                    substitution = True
                calculation = calc_new

    return calculation


def write_failed_file(filename):
    with open(filename + "_FAILED", "w") as failed_file:
        failed_file.write("FAILED")
        failed_file.close()


if __name__ == '__main__':
    main()
