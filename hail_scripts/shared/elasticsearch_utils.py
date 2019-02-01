import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO


# Elastic search write operations.
# See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#_operation
ELASTICSEARCH_INDEX = "index"
ELASTICSEARCH_CREATE = "create"
ELASTICSEARCH_UPDATE = "update"
ELASTICSEARCH_UPSERT = "upsert"
ELASTICSEARCH_WRITE_OPERATIONS = set([
    ELASTICSEARCH_INDEX, ELASTICSEARCH_CREATE, ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT,
])


# make encoded values as human-readable as possible
ES_FIELD_NAME_ESCAPE_CHAR = '$'
ES_FIELD_NAME_BAD_LEADING_CHARS = set(['_', '-', '+', ES_FIELD_NAME_ESCAPE_CHAR])
ES_FIELD_NAME_SPECIAL_CHAR_MAP = {
    '.': '_$dot$_',
    ',': '_$comma$_',
    '#': '_$hash$_',
    '*': '_$star$_',
    '(': '_$lp$_',
    ')': '_$rp$_',
    '[': '_$lsb$_',
    ']': '_$rsb$_',
    '{': '_$lcb$_',
    '}': '_$rcb$_',
}


def _encode_field_name(s):
    """Encodes arbitrary string into an elasticsearch field name

    See:
    https://discuss.elastic.co/t/special-characters-in-field-names/10658/2
    https://discuss.elastic.co/t/illegal-characters-in-elasticsearch-field-names/17196/2
    """
    field_name = StringIO()
    for i, c in enumerate(s):
        if c == ES_FIELD_NAME_ESCAPE_CHAR:
            field_name.write(2*ES_FIELD_NAME_ESCAPE_CHAR)
        elif c in ES_FIELD_NAME_SPECIAL_CHAR_MAP:
            field_name.write(ES_FIELD_NAME_SPECIAL_CHAR_MAP[c])  # encode the char
        else:
            field_name.write(c)  # write out the char as is

    field_name = field_name.getvalue()

    # escape 1st char if necessary
    if any(field_name.startswith(c) for c in ES_FIELD_NAME_BAD_LEADING_CHARS):
        return ES_FIELD_NAME_ESCAPE_CHAR + field_name
    else:
        return field_name


def _decode_field_name(field_name):
    """Converts an elasticsearch field name back to the original unencoded string"""

    if field_name.startswith(ES_FIELD_NAME_ESCAPE_CHAR):
        field_name = field_name[1:]

    i = 0
    original_string = StringIO()
    while i < len(field_name):
        current_string = field_name[i:]
        if current_string.startswith(2*ES_FIELD_NAME_ESCAPE_CHAR):
            original_string.write(ES_FIELD_NAME_ESCAPE_CHAR)
            i += 2
        else:
            for original_value, encoded_value in ES_FIELD_NAME_SPECIAL_CHAR_MAP.items():
                if current_string.startswith(encoded_value):
                    original_string.write(original_value)
                    i += len(encoded_value)
                    break
            else:
                original_string.write(field_name[i])
                i += 1

    return original_string.getvalue()
