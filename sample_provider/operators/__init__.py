import sys


class RedirectBuffer:
    def __init__(self, oldout):
        self.out = oldout
    def write(self, msg):
        msgs = msg.split
        self.out.info("!!!!!!"+msg)
    def flush(self):
        # self.out.flush()
        pass


sys.stdout = RedirectBuffer(self.log)