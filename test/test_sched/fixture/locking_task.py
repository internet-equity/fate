#!/usr/bin/env python3
import fcntl
import json
import sys

from fate import task


def log(msg, level_default='INFO'):
    if isinstance(msg, dict):
        level = msg.pop('level', level_default).upper()
    else:
        level = level_default

    task.log.log(level, msg)


def main():
    param = json.load(sys.stdin)

    msgs = param.get('logs', [])

    if msgs and (msg_head := msgs.pop(0)):
        log(msg_head)

    with open(param['lock_path'], 'w') as fd:
        fcntl.lockf(fd, fcntl.LOCK_EX)

        if msgs and (msg_body := msgs.pop(0)):
            log(msg_body)

        print(param['result'], end='')

        fcntl.lockf(fd, fcntl.LOCK_UN)

    if msgs and (msg_tail := msgs.pop(0)):
        log(msg_tail)


if __name__ == '__main__':
    main()
