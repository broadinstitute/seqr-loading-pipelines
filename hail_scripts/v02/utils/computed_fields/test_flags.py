import unittest

import hail as hl

from .flags import (
    get_expr_for_consequence_lc_lof_flag,
    get_expr_for_variant_lc_lof_flag,
    get_expr_for_genes_with_lc_lof_flag,
    get_expr_for_consequence_loftee_flag_flag,
    get_expr_for_variant_loftee_flag_flag,
    get_expr_for_genes_with_loftee_flag_flag,
)


class TestFlags(unittest.TestCase):
    def setUp(self):
        self.all_lc_lof = hl.literal(
            [
                hl.struct(gene_id="foo", lof="LC", lof_flags="", lof_info=""),
                hl.struct(gene_id="foo", lof="NC", lof_flags="", lof_info=""),
                hl.struct(gene_id="bar", lof="LC", lof_flags="", lof_info=""),
                hl.struct(gene_id="baz", lof="", lof_flags="", lof_info=""),
                hl.struct(gene_id="baz", lof="LC", lof_flags="", lof_info=""),
            ]
        )

        self.some_lc_lof = hl.literal(
            [
                hl.struct(gene_id="foo", lof="LC", lof_flags="", lof_info=""),
                hl.struct(gene_id="foo", lof="", lof_flags="", lof_info=""),
                hl.struct(gene_id="bar", lof="LC", lof_flags="", lof_info=""),
                hl.struct(gene_id="baz", lof="HC", lof_flags="", lof_info=""),
                hl.struct(gene_id="baz", lof="LC", lof_flags="", lof_info=""),
            ]
        )

        self.all_loftee_flags = hl.literal(
            [
                hl.struct(gene_id="foo", lof="HC", lof_flags="flag1", lof_info=""),
                hl.struct(gene_id="foo", lof="HC", lof_flags="flag2", lof_info=""),
                hl.struct(gene_id="bar", lof="LC", lof_flags="flag1", lof_info=""),
                hl.struct(gene_id="baz", lof="HC", lof_flags="flag2", lof_info=""),
                hl.struct(gene_id="baz", lof="", lof_flags="", lof_info=""),
            ]
        )

        self.some_loftee_flags = hl.literal(
            [
                hl.struct(gene_id="foo", lof="HC", lof_flags="flag1", lof_info=""),
                hl.struct(gene_id="foo", lof="HC", lof_flags="", lof_info=""),
                hl.struct(gene_id="bar", lof="", lof_flags="flag1", lof_info=""),
                hl.struct(gene_id="bar", lof="LC", lof_flags="flag1", lof_info=""),
                hl.struct(gene_id="baz", lof="HC", lof_flags="flag2", lof_info=""),
                hl.struct(gene_id="baz", lof="HC", lof_flags="flag3", lof_info=""),
            ]
        )

    def test_consequence_lc_lof_flag(self):
        self.assertTrue(hl.eval(get_expr_for_consequence_lc_lof_flag(hl.struct(lof="LC"))))
        self.assertFalse(hl.eval(get_expr_for_consequence_lc_lof_flag(hl.struct(lof="HC"))))
        self.assertFalse(hl.eval(get_expr_for_consequence_lc_lof_flag(hl.struct(lof=""))))

    def test_variant_lc_lof_flag(self):
        self.assertTrue(hl.eval(get_expr_for_variant_lc_lof_flag(self.all_lc_lof)))
        self.assertFalse(hl.eval(get_expr_for_variant_lc_lof_flag(self.some_lc_lof)))

    def test_genes_with_lc_lof_flag(self):
        self.assertSetEqual(hl.eval(get_expr_for_genes_with_lc_lof_flag(self.all_lc_lof)), set(["foo", "bar", "baz"]))
        self.assertSetEqual(hl.eval(get_expr_for_genes_with_lc_lof_flag(self.some_lc_lof)), set(["foo", "bar"]))

    def test_consequence_loftee_flag_flag(self):
        self.assertTrue(hl.eval(get_expr_for_consequence_loftee_flag_flag(hl.struct(lof="HC", lof_flags="foo"))))
        self.assertFalse(hl.eval(get_expr_for_consequence_loftee_flag_flag(hl.struct(lof="", lof_flags=""))))
        self.assertFalse(hl.eval(get_expr_for_consequence_loftee_flag_flag(hl.struct(lof="", lof_flags="bar"))))

    def test_variant_loftee_flag_flag(self):
        self.assertTrue(hl.eval(get_expr_for_variant_loftee_flag_flag(self.all_loftee_flags)))
        self.assertFalse(hl.eval(get_expr_for_variant_loftee_flag_flag(self.some_loftee_flags)))

    def test_genes_with_loftee_flag_flag(self):
        self.assertSetEqual(
            hl.eval(get_expr_for_genes_with_loftee_flag_flag(self.all_loftee_flags)), set(["foo", "bar", "baz"])
        )
        self.assertSetEqual(
            hl.eval(get_expr_for_genes_with_loftee_flag_flag(self.some_loftee_flags)), set(["bar", "baz"])
        )


if __name__ == "__main__":
    unittest.main()
