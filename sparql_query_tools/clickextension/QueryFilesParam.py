from pathlib import Path

import click


class QueriesFileParam(click.ParamType):
    name = "queries"

    def convert(self, value, param, ctx):
        path = Path(value)
        if not path.exists():
            self.fail("File does not exist.".format(value), param, ctx)
        elif not path.is_file():
            self.fail("Is not a file.".format(value), param, ctx)

        try:
            # check to see if file is readable
            with open(path, 'r') as f:
                f.read(1)
        except:
            self.fail("File is not readable.".format(value), param, ctx)
            # here you can modify the error message to your liking

        return path
