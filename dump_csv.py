####
# This script demonstrates how to use the Tableau Server Client
# to download a high resolution image of a view from Tableau Server.
#
# For more information, refer to the documentations on 'Query View Image'
# (https://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm)
#
# To run the script, you must have installed Python 2.7.X or 3.3 and later.
####

import argparse
import codecs
import getpass
import logging
import os
import urllib
import csv

import tableauserverclient as TSC


def main():

    parser = argparse.ArgumentParser(description='dump output from workbooks')
    parser.add_argument('--server', '-s', required=True, help='server address')
    parser.add_argument('--site', '-S', default='')
    parser.add_argument('--project', required=True, default=None, help='project in which to search workbooks')
    parser.add_argument('--username', '-u', help='username to sign into server')
    parser.add_argument('-p', '--password', default=None)
    parser.add_argument('--filepath', '-f', required=True, help='filepath to save the image(s) returned')
    parser.add_argument('--refresh', '-r', action='store_true', help='refresh the workbook before extracting data')

    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')

    parser.add_argument('workbook', help='one or more workbooks to process, "all" means all workbooks', nargs='+')

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
        # Step 2: Query for the workbook that we want data of
        for wb in TSC.Pager(server.workbooks):
            if (args.workbook[0] == "all" or wb.name in args.workbook) and wb.project_name == args.project:
                server.workbooks.populate_views(wb)
                if args.refresh:
                    try:
                        server.workbooks.refresh(wb.id)
                    except Exception as e:
                        logging.error("workbook[{0}]: refresh failed [{1}]".format(wb.name, e))
                        filename = os.path.join(args.filepath, urllib.parse.quote(wb.name, ' '))
                        write_failed_file(filename)
                        continue
                for view in wb.views:
                    # Step 3: Query the CSV endpoint and save the data to the specified location
                    server.views.populate_csv(view)
                    filename = os.path.join(args.filepath, urllib.parse.quote(wb.name, ' '), urllib.parse.quote(view.name, ' ')) + ".csv"
                    try:
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        with open(filename, "w", newline='', encoding='utf-8') as csv_file:
                            writer = csv.writer(csv_file, delimiter=';')
                            data = b''
                            for chunk in view.csv:
                                data += chunk
                            data, size = codecs.utf_8_decode(data)
                            reader = csv.reader(data.splitlines())
                            writer.writerows(reader)
                            csv_file.close()
                        logging.info("workbook[{0}], view[{1}]: CSV saved to [{2}]".format(wb.name, view.name, filename))
                    except Exception as e:
                        logging.error("workbook[{0}], view[{1}]: CSV could not be retrieved [{2}]".format(wb.name, view.name, e))
                        write_failed_file(filename)


def write_failed_file(filename):
    with open(filename + "_FAILED", "w") as failed_file:
        failed_file.write("FAILED")
        failed_file.close()


if __name__ == '__main__':
    main()
