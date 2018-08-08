import unittest

from elasticsearch_utils import _encode_field_name, _decode_field_name


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
            encoded = _encode_field_name(test_string)
            decoded = _decode_field_name(encoded)

            self.assertEqual(test_string, decoded, "encode/decode cycle broken for: %s. Encoded: %s Decoded: %s" % (test_string, encoded, decoded))

            print("%s => %s" % (test_string, encoded))

if __name__ == '__main__':
    unittest.main()
