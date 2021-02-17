import click


class URLParam(click.ParamType):
    name = "url"

    def convert(self, value, param, ctx):
        import re
        regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        if re.match(regex, value) is None:
            self.fail(
                "{} is not a valid IRI.".format(value),
                param,
                ctx,
            )
        import urllib.parse
        return urllib.parse.urlparse(value)
