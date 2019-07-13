####
# To run the script, you must have installed Python 2.7.X or 3.3 and later.
####

import sys
import signal
import time
import argparse
import getpass
import logging

import tableauserverclient as TSC


def handler(signum, frame):
    raise RuntimeError("timeout waiting for jobs to finish")

def main():
    parser = argparse.ArgumentParser(description='refresh workbooks as available on a server', fromfile_prefix_chars='@')
    parser.add_argument('--server', '-s', required=True, help='server address')
    parser.add_argument('--username', '-u', required=True, help='username to sign into server')
    parser.add_argument('-p', required=True, help='password for username, use @file.txt to read from file', default=None)
    parser.add_argument('--site', '-S', default=None)
    parser.add_argument('-d', action='store_true', help='dangerous, do not verify SSL security', default=None)
    parser.add_argument('-w', action='store_true', help='wait for the refresh to finish', default=None)
    parser.add_argument('-m', type=int, help='max wait time in seconds', default=0)
    parser.add_argument('-f', type=int, help='check frequency in seconds', default=5)

    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')

    parser.add_argument('workbook', help='one or more workbooks to refresh', nargs='+')

    args = parser.parse_args()

    if args.p is None:
        password = getpass.getpass("Password: ")
    else:
        password = args.p

    options = dict()
    if args.d == True:
        options['verify'] = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Set logging level based on user input, or error by default
    logging_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=logging_level)

    signal.signal(signal.SIGALRM, handler)

    tableau_auth = TSC.TableauAuth(args.username, password, args.site)
    server = TSC.Server(args.server)
    server.add_http_options(options)
    server.version = "2.8"

    jobs = dict()

    with server.auth.sign_in(tableau_auth):
        workbooks, pagination_item = server.workbooks.get()
        for wb in workbooks:
            logging.debug(wb.name)
            if wb.name in args.workbook:
                logging.info("{0}: {1}".format(wb.name, wb.id))
                jobs[wb.id] = (wb, server.workbooks.refresh(wb.id))
        if args.w:
            signal.alarm(args.m)
            n = 0
            while n < len(jobs):
                time.sleep(args.f)
                n = 0
                for id in jobs.keys():
                    job = server.jobs.get(jobs[id][1].id)
                    logging.debug("checking job for workbook: {0}, finish code: {1}".format(jobs[id][0].name, job.finish_code))
                    # weird, workbook refreshes result in finish code == 1 when done, not 0 like data sources
                    #if job.finish_code == '1':
                    #    raise RuntimeError("refresh job exited unexpectedly for workbook {}".format(jobs[id][0].name))
                    if job.finish_code == '1':
                        n += 1
            logging.debug("all jobs are finished")
            signal.alarm(0)


if __name__ == '__main__':
    main()
