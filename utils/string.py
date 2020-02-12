# pylint: disable=import-error

import unicodedata

import numpy as np

ALLOWED_CHARS = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ,./<>?:;\\ `~!@#$%^&*()[]{}_+-=|¥\n"
)


def remove_accents(input_str, fmt="NFKD"):
    nkfd_form = unicodedata.normalize(fmt, input_str)
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])


def clean_string(s, charset=ALLOWED_CHARS):
    if not isinstance(s, str):
        s = s.decode("utf-8")  # Ensure unicode
    s = remove_accents(s)
    s = "".join([c for c in s if c in charset])  # Only keep some special characters
    return s


def format_string(input_str):
    return remove_accents(input_str).upper()


def format_datetime_string(string):
    return string.upper().replace(".", "")


def title_to_snake(title):
    return title.lower().replace(" ", "_")


def first_valid(string, charset=ALLOWED_CHARS):
    for index, char in enumerate(string):
        if not char in charset:
            break
    return string[:index]


replace_dict = {
    "P": "0",
    "B": "1",
    "V": "1",
    "F": "2",
    "H": "2",
    "T": "3",
    "D": "3",
    "S": "4",
    "Z": "4",
    "C": "4",
    "X": "4",
    "Y": "5",
    "L": "5",
    "N": "6",
    "Ñ": "6",
    "M": "6",
    "Q": "7",
    "K": "7",
    "G": "8",
    "J": "8",
    "R": "9",
    "1": "A",
    "2": "B",
    "3": "C",
    "4": "D",
    "5": "E",
    "6": "F",
    "7": "G",
    "8": "H",
    "9": "I",
    "0": "J",
}


def phonetic_string(string):
    s = format_string(string)
    new_str = ""
    for c in s:
        new_str += replace_dict[c] if c in replace_dict else ""
    return new_str


def levenshtein_distance(seq1, seq2):
    size_x = len(seq1) + 1
    size_y = len(seq2) + 1
    matrix = np.zeros((size_x, size_y))
    for x in range(size_x):
        matrix[x, 0] = x
    for y in range(size_y):
        matrix[0, y] = y

    for x in range(1, size_x):
        for y in range(1, size_y):
            if seq1[x - 1] == seq2[y - 1]:
                matrix[x, y] = min(matrix[x - 1, y] + 1, matrix[x - 1, y - 1], matrix[x, y - 1] + 1)
            else:
                matrix[x, y] = min(matrix[x - 1, y] + 1, matrix[x - 1, y - 1] + 1, matrix[x, y - 1] + 1)
    return matrix[size_x - 1, size_y - 1]
