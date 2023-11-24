
from contextlib import redirect_stdout
import sys

real_stdout = sys.stdout


def print_formatter(output: str) -> None:
    """Handling each line of output"""
    real_stdout.write('::::: ' + output)


class WriteProcessor:

    def __init__(self):
        self.buf = ""

    def write(self, buf):
        buf = buf if type(buf) is str else str(buf, "utf-8")
        # emit on each newline
        while buf:

            newline_index = buf.find("\n")
            if newline_index <0:
                # no newline, buffer for next call
                self.buf += buf
                break
            # get data to next newline and combine with any buffered data
            data = self.buf + buf[:newline_index + 1]
            self.buf = ""
            buf = buf[newline_index + 1:]
            # perform complex calculations... or just print with a note.
            print_formatter(data)

    def flush(self):
        pass

class RedirectPrinter(redirect_stdout):
     def __init__(self):
         super().__init__(WriteProcessor())

