import unittest

from elasticsearch_utils import encode_field_name, ES_FIELD_NAME_ESCAPE_CHAR, ES_FIELD_NAME_SPECIAL_CHAR_MAP, StringIO

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


class TestElasticsearchUtils(unittest.TestCase):

    def test_field_name_encode_decode(self):
        test_strings = [
            "",
            "_",
            "+",
            "-",
            "$",
            "+_)(*&^%$#@!~",
            "~!@#$%^&*()_+",
            "_____",
            "$abcd./",
            "$$abcd./",
            "$dot$",
            "_+-$lcb$dot$--",
            ".$dot$_+-$lcb$dot$--",
            "s1.GQ",
            "s1.DP",
            "s1.2.3.DP",
            ".s1.2.3.DP",
            "-s1-2-3.DP",
        ]

        for test_string in test_strings:
            encoded = encode_field_name(test_string)
            decoded = _decode_field_name(encoded)

            self.assertEqual(test_string, decoded, "encode/decode cycle broken for: %s. Encoded: %s Decoded: %s" % (test_string, encoded, decoded))

            print("%s => %s" % (test_string, encoded))

if __name__ == '__main__':
    unittest.main()
