from functools import partial
from feature_selector import corr_selector
from feature_cleaner import outlier_replace
from feature_constructor import feature_power

corr_selector_p = partial(corr_selector, threshold=0.99)
outlier_replace_p = partial(outlier_replace, max_value=100000, min_value=0)
feature_power_p = partial(feature_power, power=2)
