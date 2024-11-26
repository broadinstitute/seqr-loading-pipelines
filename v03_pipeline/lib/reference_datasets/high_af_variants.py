import hail as hl

ONE_TENTH_PERCENT = 0.001
ONE_PERCENT = 0.01
THREE_PERCENT = 0.03
FIVE_PERCENT = 0.05
TEN_PERCENT = 0.10


def get_ht(
    ht: hl.Table,
) -> hl.Table:
    ht = ht.select_globals()
    ht = ht.filter(ht.AF_POPMAX_OR_GLOBAL > ONE_TENTH_PERCENT)
    return ht.select(
        is_gt_1_percent=ht.AF_POPMAX_OR_GLOBAL > ONE_PERCENT,
        is_gt_3_percent=ht.AF_POPMAX_OR_GLOBAL > THREE_PERCENT,
        is_gt_5_percent=ht.AF_POPMAX_OR_GLOBAL > FIVE_PERCENT,
        is_gt_10_percent=ht.AF_POPMAX_OR_GLOBAL > TEN_PERCENT,
    )
