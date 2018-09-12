import unittest
from hail_scripts.v01.utils.fam_file_utils import \
    _compute_sample_groups, \
    _check_for_extra_sample_ids_in_fam_file, \
    _check_for_extra_sample_ids_in_vds


class TestElasticsearchUtils(unittest.TestCase):

    def setUp(self):
        self.vds_sample_ids = ["a", "b", "d", "c", "e", "f", "g", "h", "i"]
        self.individual_id_to_family_id = {
            "a": "1", "b": "1", "c": "1",
            "d": "2", "e": "2",
            "f": "3", "g": "3", "h": "3", "i": "3",
        }

    def test_compute_sample_groups(self):

        for num_samples_per_index in [1, 2]:
            sample_groups = _compute_sample_groups(
                self.vds_sample_ids,
                self.individual_id_to_family_id,
                num_samples_per_index)
            self.assertEqual(len(sample_groups), 3)
            self.assertListEqual(sample_groups[0], ["a", "b", "c"])
            self.assertListEqual(sample_groups[1], ["d", "e"])
            self.assertListEqual(sample_groups[2], ["f", "g", "h", "i"])

        num_samples_per_index = 3
        sample_groups = _compute_sample_groups(
            self.vds_sample_ids,
            self.individual_id_to_family_id,
            num_samples_per_index)
        self.assertEqual(len(sample_groups), 2)
        self.assertListEqual(sample_groups[0], ["a", "b", "c"])
        self.assertListEqual(sample_groups[1], ["d", "e"] + ["f", "g", "h", "i"])

        for num_samples_per_index in [4, 5]:
            sample_groups = _compute_sample_groups(
                self.vds_sample_ids,
                self.individual_id_to_family_id,
                num_samples_per_index)
            self.assertEqual(len(sample_groups), 2)
            self.assertListEqual(sample_groups[0], ["a", "b", "c"]+["d", "e"] )
            self.assertListEqual(sample_groups[1], ["f", "g", "h", "i"])

        for num_samples_per_index in [6, 10]:
            sample_groups = _compute_sample_groups(
                self.vds_sample_ids,
                self.individual_id_to_family_id,
                num_samples_per_index)
            self.assertEqual(len(sample_groups), 1)
            self.assertListEqual(sample_groups[0], ["a", "b", "c"]+["d", "e"]+["f", "g", "h", "i"])

    def test_check_for_extra_sample_ids(self):
        # these shouldn't raise an error
        _check_for_extra_sample_ids_in_vds(self.vds_sample_ids, self.individual_id_to_family_id)
        _check_for_extra_sample_ids_in_fam_file(self.vds_sample_ids, self.individual_id_to_family_id)

        # these should
        self.assertRaises(ValueError, _check_for_extra_sample_ids_in_vds, self.vds_sample_ids + ["x"], self.individual_id_to_family_id)

        self.individual_id_to_family_id["x"] = "1"
        self.assertRaises(ValueError, _check_for_extra_sample_ids_in_fam_file, self.vds_sample_ids, self.individual_id_to_family_id)

if __name__ == '__main__':
    unittest.main()
