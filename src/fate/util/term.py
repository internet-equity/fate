import sys
import tty
import termios


def getch(n=1, /, file=sys.stdin):
    """Get n character(s) from TTY file."""
    fd = file.fileno()
    old = termios.tcgetattr(fd)

    try:
        tty.setraw(fd)
        return file.read(n)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def getche(n=1, /, infile=sys.stdin, outfile=sys.stdout, end=''):
    """Print back input from getch."""
    result = getch(n, infile)
    print(result, end=end, file=outfile)
    return result
