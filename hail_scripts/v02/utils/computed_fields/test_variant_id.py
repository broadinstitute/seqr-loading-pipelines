import unittest

import hail as hl

from .variant_id import get_expr_for_xpos


class TestXpos(unittest.TestCase):
    def test_xpos_1(self):
        locus = hl.parse_locus("1:55505463", "GRCh37")
        self.assertEqual(hl.eval(get_expr_for_xpos(locus)), 1055505463)

    def test_xpos_2(self):
        locus = hl.parse_locus("X:18525192", "GRCh37")
        self.assertEqual(hl.eval(get_expr_for_xpos(locus)), 23018525192)

    def test_xpos_grch38(self):
        locus = hl.parse_locus("chr2:166847734", "GRCh38")
        self.assertEqual(hl.eval(get_expr_for_xpos(locus)), 2166847734)


if __name__ == "__main__":
    unittest.main()
