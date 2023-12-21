from functools import partial

import numpy as np

distribution_mapping = {
    "normal": partial(np.random.normal, loc=10.0, scale=5.0),
    "poisson": partial(np.random.poisson, lam=1.0),
    "laplace": partial(np.random.laplace, loc=0.0, scale=1.0),
    "geometric": partial(np.random.geometric, p=0.3),
    "binomial": partial(np.random.binomial, n=10, p=0.3),
}
KAFKA_HOSTS = ["localhost:9092"]
