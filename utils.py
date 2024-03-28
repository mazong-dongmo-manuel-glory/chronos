import re


def is_valid_domain(domain):
    # Regular expression for matching domain names
    pattern = r'^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,6}$'

    # Compile the regular expression pattern
    regex = re.compile(pattern)

    # Check if the domain matches the pattern
    if regex.match(domain):
        return True
    else:
        return False


def is_valid_url(url):
    # Regular expression for matching URLs
    pattern = r'^(https?):\/\/([a-zA-Z0-9\-\.]+)\.([a-zA-Z]{2,})(:[0-9]+)?(\/.*)?$'

    # Compile the regular expression pattern
    regex = re.compile(pattern)

    # Check if the URL matches the pattern
    if regex.match(url):
        return True
    else:
        return False


