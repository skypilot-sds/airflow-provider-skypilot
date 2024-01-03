import logging
import re


class SkyLogFilter(logging.Filter):
    """SkyPilot prints and logs output massages with ANSI color codes, but logs in Airflow cannot interpret them.
    This class remove the color codes for more clean representation.

    Example:
        logger.addFilter(SkyLogFilter())
    """
    ansi_escape = re.compile(r'(?:\x1B[@-Z\\-_]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])')

    def _remove_color_code(self, msg):
        return self.ansi_escape.sub('', msg)

    def _arrange_multi_lines(self, msg):
        """for clear alignment for multi-line logs."""
        msg = msg.rstrip()
        if '\n' in msg or '\r' in msg:
            msg = '\n' + msg.lstrip()
            msg = msg.replace("\r\n", "\n")
            msg = msg.replace("\r", "\n")
            msg = msg.replace("\n", "\n\t\t")

        return msg

    def filter(self, record):
        msg = self._remove_color_code(record.msg)
        msg = self._arrange_multi_lines(msg)
        record.msg = msg
        return super().filter(record)

