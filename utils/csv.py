# pylint: disable=no-name-in-module
# pylint: disable=import-error

import re
import csv
from multiprocessing import cpu_count, Pool

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

from utils.string import clean_string, title_to_snake, first_valid

URL_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-_~+#,%&=*;:@/"
EXTRA_STOPWORDS = {
    "another",
    "april",
    "august",
    "bug",
    "call",
    "core",
    "community.nxp.com",
    "december",
    "down",
    "eight",
    "existing",
    "february",
    "five",
    "fix",
    "follow",
    "four",
    "friday",
    "help",
    "january",
    "july",
    "june",
    "left",
    "list",
    "march",
    "monday",
    "new",
    "nine",
    "november",
    "october",
    "one",
    "order",
    "plan",
    "research",
    "review",
    "right",
    "saturday",
    "september",
    "seven",
    "six",
    "start",
    "stop",
    "sunday",
    "ten",
    "three",
    "thursday",
    "tuesday",
    "two",
    "up",
    "using",
    "view",
    "wednesday",
}


def line_generator(lines):
    for line in lines:
        yield line


def clean_headers(headers):
    headers = [clean_string(h) for h in headers]
    headers = [title_to_snake(h) for h in headers]
    return headers


is_empty = lambda s: not any(s.replace("'", "").replace('"', "").replace("\n", "").split(","))


def chunk_generator(lines, n=cpu_count() * 4):
    g = line_generator(lines)
    try:
        while True:
            lines = []
            for _ in range(n):
                lines.append(next(g))
            yield lines
    except StopIteration:
        if len(lines) > 0:
            yield lines
        return


def filter_empty(lines):
    return [line for line in lines if not is_empty(line)]


def get_stopwords(default_stopwords=set(stopwords.words("english")), extra_stopwords=EXTRA_STOPWORDS):
    return default_stopwords.union(extra_stopwords)


def remove_stopwords(line, stop_words=get_stopwords()):
    words = word_tokenize(line)
    words = [word for word in words if word.lower() not in stop_words]
    return fix_spaces(" ".join(words))


def fix_spaces(text, charset='":;,.<>/?[]{}\\|"'):
    for char in charset:
        text = text.replace(" " + char, char).replace(char + " ", char)
    return text.strip()


def clean_csv(dirty_path, clean_path):
    reader = csv.reader(open(dirty_path))
    writer = csv.writer(open(clean_path, "w"))
    headers = clean_headers(next(reader))
    writer.writerow(headers)
    for line in reader:
        new_line = [remove_stopwords(cell.lower()) for cell in line]
        if any(new_line):
            writer.writerow(new_line)


def find_urls(lines):
    urls = set()
    for line in lines:
        for text in line:
            text_urls = {url for url in url_generator(text)}
            urls = urls.union(text_urls)
    return urls


def url_generator(s):
    matches = list(re.finditer("http", s))
    matches.reverse()
    for match in matches:
        index = match.start()
        sufix = s[index:]
        s = s[:index]
        yield first_valid(sufix, URL_CHARS)

